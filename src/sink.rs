use apalis_core::backend::future::BoxSyncFuture;
use async_nats::{
    HeaderMap,
    jetstream::{
        consumer::{Consumer, FromConsumer, IntoConsumerConfig},
        message::PublishMessage,
    },
};
use core::fmt;
use futures::{FutureExt, Sink, future::BoxFuture};
use std::{
    collections::VecDeque,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};
use ulid::Ulid;

use crate::{JetStreamTask, NatsJetStream, State, consumer::IntoMessageStream, error::Error};

pub(crate) struct JetStreamSink<T> {
    items: VecDeque<JetStreamTask>,
    pending_sends: VecDeque<BoxSyncFuture<Result<(), Error>>>,
    marker: std::marker::PhantomData<T>,
}

impl<T> fmt::Debug for JetStreamSink<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JetStreamSink")
            .field("items", &self.items)
            .field(
                "pending_sends",
                &self.pending_sends.iter().map(|_| "..").collect::<Vec<_>>(),
            )
            .field("marker", &self.marker)
            .finish()
    }
}

impl<T> Default for JetStreamSink<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> JetStreamSink<T> {
    pub(crate) fn new() -> Self {
        Self {
            items: VecDeque::new(),
            pending_sends: VecDeque::new(),
            marker: PhantomData,
        }
    }
}

impl<T> Clone for JetStreamSink<T> {
    fn clone(&self) -> Self {
        Self {
            items: VecDeque::new(),
            pending_sends: VecDeque::new(),
            marker: PhantomData,
        }
    }
}

impl<T, C, Stm> Sink<JetStreamTask> for NatsJetStream<T, C>
where
    T: Send + 'static + Unpin,
    C: IntoConsumerConfig + FromConsumer + Clone + Unpin + Send + 'static,
    Consumer<C>: IntoMessageStream<Stream = Stm>,
    Stm: Unpin,
{
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = &mut self.get_mut().sink;

        // Poll pending sends
        while let Some(pending) = this.pending_sends.front_mut() {
            match pending.poll_unpin(cx) {
                Poll::Ready(Ok(_msg_ids)) => {
                    this.pending_sends.pop_front();
                }
                Poll::Ready(Err(e)) => {
                    this.pending_sends.pop_front();
                    return Poll::Ready(Err(e));
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }

        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: JetStreamTask) -> Result<(), Self::Error> {
        let this = &mut self.get_mut().sink;

        this.items.push_back(item);
        Ok(())
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.as_mut().get_mut();
        loop {
            match &mut this.state {
                State::Init => {
                    let ctx = this.context.clone();
                    let cfg = this.config.clone();
                    this.state = State::CreateStream(
                        async move { Self::create_stream(&ctx, cfg).await }
                            .boxed()
                            .into(),
                    );
                }

                State::CreateStream(fut) => match fut.poll_unpin(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(e)) => {
                        self.state = State::Init;
                        return Poll::Ready(Err(e));
                    }
                    Poll::Ready(Ok(stream)) => {
                        let cfg = this.config.clone();
                        this.state = State::CreateConsumer(
                            async move { Self::start_consume(&stream, cfg).await }
                                .boxed()
                                .into(),
                        );
                    }
                },
                State::CreateConsumer(fut) => match fut.poll_unpin(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(e)) => {
                        self.state = State::Init;
                        return Poll::Ready(Err(e));
                    }
                    Poll::Ready(Ok(stream)) => {
                        this.messages = Some(stream);
                        this.state = State::Running
                    }
                },
                _ => {
                    break;
                }
            }
        }

        let mut messages = Vec::new();

        while let Some(item) = this.sink.items.pop_front() {
            let queue_name = this.config.stream.name.clone();

            let mut headers = HeaderMap::from_iter(
                item.metadata()
                    .iter()
                    .map(|(k, v)| (k.as_str().try_into().unwrap(), v.as_str().into())),
            );

            let task_id = item
                .task_id()
                .map(|a| a.to_string())
                .unwrap_or(Ulid::generate().to_string());

            let has_schedule_header = headers.get("Nats-Schedule").is_some();

            let subject = match item.run_at() {
                _ if has_schedule_header && this.config.can_schedule() => {
                    format!("{queue_name}.schedules.{task_id}")
                }
                Some(_) | None if !this.config.can_schedule() => {
                    if item.run_at().is_some() || has_schedule_header {
                        tracing::warn!(
                            "Tried to schedule a job without scheduling on. Falling back"
                        );
                    }
                    queue_name
                }
                Some(run_at) => {
                    let at = unix_to_rfc3339(run_at);
                    headers.insert("Nats-Schedule", format!("@at {at}").as_str());
                    headers.insert("Nats-Schedule-Target", queue_name.as_str());
                    format!("{queue_name}.schedules.{task_id}")
                }
                None => queue_name,
            };

            let mut publish = PublishMessage::build()
                .headers(headers)
                .message_id(&task_id);
            let bytes = item.args;
            publish = publish.payload(bytes.into());
            let context = this.context.clone();

            let fut: BoxFuture<'static, Result<(), Error>> = async move {
                let _ = context.send_publish(subject, publish).await?;
                Ok(())
            }
            .boxed();
            messages.push(fut);
        }

        // Create a single pending send for all messages
        if !messages.is_empty() {
            let future = async move {
                futures::future::try_join_all(messages).await?;
                Ok(())
            }
            .boxed()
            .into();

            this.sink.pending_sends.push_back(future);
        }

        // Now poll all pending sends
        while let Some(pending) = this.sink.pending_sends.front_mut() {
            match pending.poll_unpin(cx) {
                Poll::Ready(Ok(_)) => {
                    this.sink.pending_sends.pop_front();
                }
                Poll::Ready(Err(e)) => {
                    this.sink.pending_sends.pop_front();
                    return Poll::Ready(Err(e));
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }

        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_flush(cx)
    }
}

/// Formats Unix seconds as RFC 3339 UTC, e.g. "2026-10-01T12:00:00Z".
fn unix_to_rfc3339(secs: u64) -> String {
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let (hour, min, sec) = (rem / 3_600, (rem % 3_600) / 60, rem % 60);

    // Civil-from-days (Howard Hinnant's algorithm)
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097); // [0, 146096]
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let day = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let month = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    let year = if month <= 2 { y + 1 } else { y };

    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}Z")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_rfc3339() {
        assert_eq!(unix_to_rfc3339(0), "1970-01-01T00:00:00Z");
        assert_eq!(unix_to_rfc3339(951_782_400), "2000-02-29T00:00:00Z");
        assert_eq!(unix_to_rfc3339(1_700_000_000), "2023-11-14T22:13:20Z");
    }
}
