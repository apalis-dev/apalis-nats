use std::fmt::Debug;

use apalis_core::{error::BoxDynError, task::Parts, worker::ext::ack::Acknowledge};
use async_nats::{
    Subject,
    jetstream::consumer::{Consumer, IntoConsumerConfig},
};
use futures::{
    FutureExt,
    future::{self, BoxFuture},
};
use ulid::Ulid;

use crate::{JetStreamContext, NatsJetStream, consumer::IntoMessageStream, error::JetStreamError};

impl<T, Decode, Res, C, PollErr> Acknowledge<Res, JetStreamContext, Ulid>
    for NatsJetStream<T, Decode, C>
where
    T: Send,
    Res: Debug + Send + Sync,
    Decode: Send,
    C: IntoConsumerConfig,
    Consumer<C>: IntoMessageStream<Error = PollErr>,
    PollErr: Send + 'static,
{
    type Error = JetStreamError<PollErr>;

    type Future = BoxFuture<'static, Result<(), Self::Error>>;

    fn ack(
        &mut self,
        res: &Result<Res, BoxDynError>,
        parts: &Parts<JetStreamContext, Ulid>,
    ) -> Self::Future {
        let reply: Subject = parts.ctx.reply.clone().expect("Missing ack subject");
        let client = self.client.clone();
        if res.is_ok() {
            let fut = async move {
                client
                    .publish(reply, "".into())
                    .await
                    .map_err(JetStreamError::AckError)?;
                Ok(())
            };
            return fut.boxed();
        }
        future::ready(Ok(())).boxed()
    }
}
