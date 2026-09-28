use std::fmt::Debug;

use apalis_core::{
    error::{BoxDynError, RetryAfterError},
    task::{ExecutionContext, status::Status},
    worker::ext::ack::Acknowledge,
};
use async_nats::{Subject, jetstream::Context};
use futures::{
    FutureExt,
    future::{self, BoxFuture},
};

use crate::{NatsTaskContext, error::Error};

#[derive(Debug, Clone)]
pub struct NatsJetAck {
    pub(crate) context: Context,
}

impl<Res> Acknowledge<Res> for NatsJetAck
where
    Res: Debug + Send + Sync,
{
    type Error = Error;

    type Future = BoxFuture<'static, Result<(), Self::Error>>;

    fn ack(&mut self, res: &Result<Res, BoxDynError>, ctx: &ExecutionContext) -> Self::Future {
        let reply: Subject = match ctx
            .data()
            .get::<NatsTaskContext>()
            .and_then(|c| c.reply.clone())
        {
            Some(r) => r,
            None => return future::ready(Ok(())).boxed(),
        };
        let context = self.context.clone();

        let payload: Vec<u8> = match ctx.status() {
            Status::Done => b"+ACK".to_vec(),
            Status::Killed => b"+TERM".to_vec(),
            Status::Failed => match res
                .as_ref()
                .err()
                .and_then(|e| e.downcast_ref::<RetryAfterError>())
            {
                // Retry after the delay the handler asked for
                Some(retry) => {
                    format!(r#"-NAK {{"delay": {}}}"#, retry.get_duration().as_nanos()).into_bytes()
                }
                None => b"-NAK".to_vec(),
            },
            _ => unreachable!("Invalid Status"),
        };

        async move {
            context.publish(reply, payload.into()).await?;
            Ok(())
        }
        .boxed()
    }
}
