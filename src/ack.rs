use std::fmt::Debug;

use apalis_core::{error::BoxDynError, task::Parts, worker::ext::ack::Acknowledge};
use futures::{
    FutureExt,
    future::{self, BoxFuture},
};

use crate::{JetStreamContext, NatsJetStream, error::JetStreamError};

impl<T, C, Res> Acknowledge<Res, JetStreamContext, i64> for NatsJetStream<T, C>
where
    T: Send,
    Res: Debug + Send + Sync,
    C: Send,
{
    type Error = JetStreamError;

    type Future = BoxFuture<'static, Result<(), Self::Error>>;

    fn ack(
        &mut self,
        res: &Result<Res, BoxDynError>,
        parts: &Parts<JetStreamContext, i64>,
    ) -> Self::Future {
        let reply = parts.ctx.reply.clone().unwrap();
        let client = self.client.clone();
        if res.is_ok() {
            let fut = async move {
                client
                    .publish(reply, "".into())
                    .await
                    .map_err(JetStreamError::AckError)
            };
            return fut.boxed();
        }
        future::ready(Ok(())).boxed()
    }
}
