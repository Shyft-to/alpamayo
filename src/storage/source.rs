use {
    crate::{
        config::{ConfigSource, ConfigSourceStream},
        source::{
            block::BlockWithBinary,
            http::{GetBlockError, HttpSource},
            stream::{StreamSource, StreamSourceMessage},
        },
        storage::slots::StoredSlots,
    },
    futures::stream::StreamExt,
    solana_clock::Slot,
    solana_rpc_client_api::client_error::Error as ClientError,
    std::sync::Arc,
    thiserror::Error,
    tokio::{
        sync::{Notify, mpsc, oneshot},
        time::sleep,
    },
    tokio_util::sync::CancellationToken,
    tracing::error,
};

#[derive(Debug)]
pub enum HttpRequest {
    Slots {
        tx: oneshot::Sender<Result<(Slot, Slot), ClientError>>,
    },
    FirstAvailableBlock {
        tx: oneshot::Sender<Result<Slot, ClientError>>,
    },
    Block {
        slot: Slot,
        httpget: bool,
        tx: oneshot::Sender<Result<BlockWithBinary, GetBlockError>>,
    },
}

#[derive(Debug, Error)]
pub enum HttpSourceConnectedError<E> {
    #[error("send channel is closed")]
    SendError,
    #[error("recv channel is closed")]
    RecvError,
    #[error(transparent)]
    Error(#[from] E),
}

pub type HttpSourceConnectedResult<T, E> = Result<T, HttpSourceConnectedError<E>>;

#[derive(Debug, Clone)]
pub struct HttpSourceConnected {
    http_tx: mpsc::Sender<HttpRequest>,
}

impl HttpSourceConnected {
    pub fn new() -> (Self, mpsc::Receiver<HttpRequest>) {
        let (http_tx, http_rx) = mpsc::channel(1);
        let this = Self { http_tx };
        (this, http_rx)
    }

    async fn send<T, E>(
        &self,
        request: HttpRequest,
        rx: oneshot::Receiver<Result<T, E>>,
    ) -> HttpSourceConnectedResult<T, E> {
        if self.http_tx.send(request).await.is_err() {
            Err(HttpSourceConnectedError::SendError)
        } else {
            match rx.await {
                Ok(Ok(result)) => Ok(result),
                Ok(Err(error)) => Err(HttpSourceConnectedError::Error(error)),
                Err(_) => Err(HttpSourceConnectedError::RecvError),
            }
        }
    }

    pub async fn get_slots(&self) -> HttpSourceConnectedResult<(Slot, Slot), ClientError> {
        let (tx, rx) = oneshot::channel();
        self.send(HttpRequest::Slots { tx }, rx).await
    }

    pub async fn get_block(
        &self,
        slot: Slot,
        httpget: bool,
    ) -> HttpSourceConnectedResult<BlockWithBinary, GetBlockError> {
        let (tx, rx) = oneshot::channel();
        self.send(HttpRequest::Block { slot, httpget, tx }, rx)
            .await
    }

    pub async fn get_first_available_block(&self) -> HttpSourceConnectedResult<Slot, ClientError> {
        let (tx, rx) = oneshot::channel();
        self.send(HttpRequest::FirstAvailableBlock { tx }, rx).await
    }
}

pub async fn start(
    config: ConfigSource,
    mut http_rx: mpsc::Receiver<HttpRequest>,
    stream_start: Arc<Notify>,
    stream_tx: mpsc::Sender<StreamSourceMessage>,
    shutdown: CancellationToken,
    index_vote: bool,
    stored_slots: StoredSlots,
) -> anyhow::Result<()> {
    let http = Arc::new(HttpSource::new(config.http, index_vote).await?);
    let stream = start_stream(
        config.stream,
        stream_tx,
        stream_start,
        index_vote,
        stored_slots,
    );

    tokio::pin!(shutdown);
    tokio::pin!(stream);

    let mut finished = false;
    while !finished {
        finished = tokio::select! {
            () = shutdown.cancelled() => true,
            item = http_rx.recv() => handle_http(item, Arc::clone(&http)),
            result = &mut stream => return result,
        };
    }
    shutdown.cancel();

    Ok(())
}

async fn start_stream(
    config: ConfigSourceStream,
    stream_tx: mpsc::Sender<StreamSourceMessage>,
    stream_start: Arc<Notify>,
    index_vote: bool,
    stored_slots: StoredSlots,
) -> anyhow::Result<()> {
    let mut backoff_duration = config.reconnect.map(|c| c.backoff_max);
    let backoff_max = config.reconnect.map(|c| c.backoff_max).unwrap_or_default();
    let from_slot_max_attempts = config
        .reconnect
        .map(|c| c.from_slot_max_attempts)
        .unwrap_or(2);
    // persists across reconnects: a from_slot that keeps failing (either to
    // connect, or by erroring out before ever delivering a message, e.g. the
    // requested slot fell out of the node's replay buffer) must eventually
    // be given up on, not retried forever
    let mut from_slot_attempts = 0u8;

    stream_start.notified().await;
    'outer: loop {
        let confirmed_slot = stored_slots.confirmed_load();
        let from_slot_exhausted =
            confirmed_slot != Slot::MIN && from_slot_attempts >= from_slot_max_attempts;
        let from_slot =
            (confirmed_slot != Slot::MIN && !from_slot_exhausted).then_some(confirmed_slot + 1);

        if from_slot_exhausted {
            // from_slot resume gave up: let the write side catch up via rpc
            // first, then connect live, same as before from_slot resume
            // existed at all
            if stream_tx
                .send(StreamSourceMessage::CatchupRequired)
                .await
                .is_err()
            {
                error!("failed to send a message to the stream");
                return Ok(());
            }
            stream_start.notified().await;
            from_slot_attempts = 0;
        }

        let mut stream = loop {
            match StreamSource::new(config.clone(), index_vote, from_slot).await {
                Ok(stream) => break stream,
                Err(error) => {
                    let mut just_exhausted = false;
                    if from_slot.is_some() {
                        from_slot_attempts += 1;
                        just_exhausted = from_slot_attempts >= from_slot_max_attempts;
                    }
                    if let Some(sleep_duration) = backoff_duration {
                        error!(?error, "failed to connect to gRPC stream");
                        sleep(sleep_duration).await;
                        backoff_duration = Some(backoff_max.min(sleep_duration * 2));
                    } else {
                        return Err(error.into());
                    }
                    if just_exhausted {
                        // catch up via rpc before retrying, instead of
                        // silently falling back to a live connect here
                        continue 'outer;
                    }
                }
            }
        };
        let from_slot_resumed = from_slot.is_some();
        if stream_tx.send(StreamSourceMessage::Start).await.is_err() {
            error!("failed to send a message to the stream");
            return Ok(());
        }

        let mut received_message = false;
        loop {
            match stream.next().await {
                Some(Ok(message)) => {
                    received_message = true;
                    from_slot_attempts = 0;
                    if stream_tx.send(message).await.is_err() {
                        error!("failed to send a message to the stream");
                        return Ok(());
                    }
                }
                Some(Err(error)) => {
                    error!(?error, "gRPC stream error");
                    if from_slot_resumed && !received_message {
                        from_slot_attempts += 1;
                    }
                    break;
                }
                None => {
                    error!("gRPC stream is finished");
                    break;
                }
            }
        }

        if let Some(config) = config.reconnect {
            // throttle even when the stream connected fine and only errored
            // afterwards (e.g. from_slot out of range) - otherwise this spins
            // as fast as the server can reject us
            sleep(config.backoff_init).await;
            backoff_duration = Some(config.backoff_init);
        } else {
            return Ok(());
        }
    }
}

fn handle_http(item: Option<HttpRequest>, http: Arc<HttpSource>) -> bool {
    match item {
        Some(request) => {
            tokio::spawn(async move {
                match request {
                    HttpRequest::Slots { tx } => {
                        let result =
                            tokio::try_join!(http.get_finalized_slot(), http.get_confirmed_slot());
                        let _ = tx.send(result);
                    }
                    HttpRequest::FirstAvailableBlock { tx } => {
                        let result = http.get_first_available_block().await;
                        let _ = tx.send(result);
                    }
                    HttpRequest::Block { slot, httpget, tx } => {
                        let result = http.get_block(slot, httpget).await;
                        let _ = tx.send(result);
                    }
                }
            });
            false
        }
        None => {
            error!("RPC requests stream is finished");
            true
        }
    }
}
