use {
    crate::{
        config::{ConfigRpc, ConfigRpcCallJson},
        metrics::RPC_WORKERS_CPU_SECONDS_TOTAL,
        rpc::{upstream::RpcClientJsonrpc, workers::WorkRequest},
        storage::{
            read::{
                ReadRequest, ReadResultBlock, ReadResultBlockHeight, ReadResultBlockTime,
                ReadResultBlockhashValid, ReadResultBlocks, ReadResultInflationReward,
                ReadResultLatestBlockhash, ReadResultRecentPrioritizationFees,
                ReadResultSignaturePosition, ReadResultSignatureStatuses,
                ReadResultSignaturesForAddress, ReadResultTransaction,
                ReadResultTransactionsForAddress,
            },
            rocksdb::{
                InflationRewardBaseValue, ReadRequestResultInflationReward,
                RocksdbWriteInflationReward,
            },
            slots::StoredSlots,
        },
        util::HashMap,
    },
    crossbeam::channel::{Sender, TrySendError},
    futures::future::{BoxFuture, try_join_all},
    jsonrpsee_types::{
        Extensions, Id, Params, Request, Response, ResponsePayload, TwoPointZero,
        error::{ErrorObjectOwned, INVALID_PARAMS_MSG},
    },
    metrics::gauge,
    prost::Message,
    richat_metrics::duration_to_seconds,
    richat_shared::jsonrpc::{
        helpers::{
            jsonrpc_error_invalid_params, jsonrpc_response_error, jsonrpc_response_error_custom,
            jsonrpc_response_success, to_vec,
        },
        requests::{RpcRequestResult, RpcRequestsProcessor},
    },
    serde::{Deserialize, Serialize, de},
    serde_json::json,
    solana_clock::{Epoch, Slot, UnixTimestamp},
    solana_commitment_config::{CommitmentConfig, CommitmentLevel},
    solana_epoch_rewards_hasher::EpochRewardsHasher,
    solana_epoch_schedule::EpochSchedule,
    solana_hash::Hash,
    solana_pubkey::Pubkey,
    solana_rpc_client::{
        http_sender::HttpSender, nonblocking::rpc_client::RpcClient, rpc_client::RpcClientConfig,
    },
    solana_rpc_client_api::{
        config::{
            RpcBlockConfig, RpcBlocksConfigWrapper, RpcContextConfig, RpcEncodingConfigWrapper,
            RpcEpochConfig, RpcLeaderScheduleConfig, RpcLeaderScheduleConfigWrapper,
            RpcSignatureStatusConfig, RpcSignaturesForAddressConfig, RpcTransactionConfig,
        },
        custom_error::RpcCustomError,
        request::{MAX_GET_CONFIRMED_BLOCKS_RANGE, MAX_GET_SIGNATURE_STATUSES_QUERY_ITEMS},
        response::{
            Response as RpcResponse, RpcBlockhash, RpcConfirmedTransactionStatusWithSignature,
            RpcInflationReward, RpcResponseContext, RpcVersionInfo,
        },
    },
    solana_signature::Signature,
    solana_storage_proto::convert::generated,
    solana_transaction::sanitized::MAX_TX_ACCOUNT_LOCKS,
    solana_transaction_status::{
        BlockEncodingOptions, ConfirmedBlock, ConfirmedTransactionWithStatusMeta,
        EncodedTransactionWithStatusMeta, Reward, RewardType, TransactionDetails,
        TransactionStatus, TransactionWithStatusMeta, UiConfirmedBlock, UiTransactionEncoding,
    },
    std::{
        future::Future,
        str::FromStr,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicU64, Ordering},
        },
        time::{Duration, Instant},
    },
    tokio::{
        sync::{mpsc, oneshot},
        task::spawn_blocking,
        time::sleep,
    },
    tracing::{debug, error},
};

// reasons why a request is forwarded to upstream, used as the "reason" metric label
const REASON_BELOW_RETENTION: &str = "below_retention"; // slot/data fell outside local storage retention window
const REASON_REMOVED: &str = "removed"; // local storage evicted the slot while the read was in flight
const REASON_NOT_FOUND_LOCALLY: &str = "not_found_locally"; // not present in local storage at all
const REASON_INCOMPLETE_RESULTS: &str = "incomplete_results"; // local storage couldn't fill the requested limit
const REASON_MISSING_STATUS: &str = "missing_status"; // signature status missing locally and history search requested
const REASON_ALWAYS: &str = "always"; // method has no local source, always served from upstream

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcRecentPrioritizationFeesConfig {
    pub percentile: Option<u16>,
}

#[derive(Debug)]
pub struct State {
    epoch_schedule: EpochSchedule,
    stored_slots: StoredSlots,
    request_timeout: Duration,
    gsfa_limit: usize,
    gtfa_limit_signatures: usize,
    gtfa_limit_full: usize,
    gss_transaction_history: bool,
    grpf_percentile: bool,
    requests_tx: mpsc::Sender<ReadRequest>,
    db_write_inflation_reward: RocksdbWriteInflationReward,
    upstreams: Vec<RpcClientJsonrpc>,
    workers: Sender<WorkRequest>,
    cluster_processed_slot: Arc<AtomicU64>,
    health_check_slot_distance: u64,
    health_disabled: Arc<AtomicBool>,
}

impl State {
    pub fn new(
        config: ConfigRpc,
        stored_slots: StoredSlots,
        requests_tx: mpsc::Sender<ReadRequest>,
        db_write_inflation_reward: RocksdbWriteInflationReward,
        workers: Sender<WorkRequest>,
        cluster_processed_slot: Arc<AtomicU64>,
        health_disabled: Arc<AtomicBool>,
    ) -> anyhow::Result<Self> {
        let upstreams = config
            .upstream_jsonrpc
            .into_iter()
            .map(|config_upstream| RpcClientJsonrpc::new(config_upstream, config.gcn_cache_ttl))
            .collect::<Result<Vec<_>, _>>()?;

        if config
            .calls_jsonrpc
            .contains(&ConfigRpcCallJson::GetClusterNodes)
            || config
                .calls_jsonrpc
                .contains(&ConfigRpcCallJson::GetLeaderSchedule)
        {
            anyhow::ensure!(
                upstreams
                    .iter()
                    .any(|upstream| upstream.is_supported(ConfigRpcCallJson::GetClusterNodes))
                    && upstreams
                        .iter()
                        .any(|upstream| upstream.is_supported(ConfigRpcCallJson::GetLeaderSchedule)),
                "at least one upstream should support `getClusterNodes` and `getLeaderSchedule`"
            );
        }

        Ok(Self {
            epoch_schedule: EpochSchedule::without_warmup(),
            stored_slots,
            request_timeout: config.request_timeout,
            gsfa_limit: config.gsfa_limit,
            gtfa_limit_signatures: config.gtfa_limit_signatures,
            gtfa_limit_full: config.gtfa_limit_full,
            gss_transaction_history: config.gss_transaction_history,
            grpf_percentile: config.grpf_percentile,
            requests_tx,
            db_write_inflation_reward,
            upstreams,
            workers,
            cluster_processed_slot,
            health_check_slot_distance: config.health_check.slot_distance,
            health_disabled,
        })
    }

    fn get_upstream(&self, call: ConfigRpcCallJson) -> Option<&RpcClientJsonrpc> {
        self.upstreams
            .iter()
            .find(|upstream| upstream.is_supported(call))
    }
}

pub fn create_request_processor(
    config: ConfigRpc,
    stored_slots: StoredSlots,
    requests_tx: mpsc::Sender<ReadRequest>,
    db_write_inflation_reward: RocksdbWriteInflationReward,
    workers: Sender<WorkRequest>,
    health_disabled: Arc<AtomicBool>,
) -> anyhow::Result<RpcRequestsProcessor<Arc<State>>> {
    let calls = &config.calls_jsonrpc;
    let needs_poller =
        calls.contains(&ConfigRpcCallJson::GetHealth) && config.health_check.rpc_uri.is_some();
    let state = State::new(
        config.clone(),
        stored_slots,
        requests_tx,
        db_write_inflation_reward,
        workers,
        Arc::new(AtomicU64::new(0)),
        health_disabled,
    )?;
    let poller_slot = needs_poller.then(|| Arc::clone(&state.cluster_processed_slot));
    let mut processor =
        RpcRequestsProcessor::new(config.body_limit, Arc::new(state), config.extra_headers);
    if calls.contains(&ConfigRpcCallJson::GetBlock) {
        processor.add_handler("getBlock", Box::new(RpcRequestBlock::handle));
    }
    if calls.contains(&ConfigRpcCallJson::GetBlockHeight) {
        processor.add_handler("getBlockHeight", Box::new(RpcRequestBlockHeight::handle));
    }
    if calls.contains(&ConfigRpcCallJson::GetBlocks) {
        processor.add_handler("getBlocks", Box::new(RpcRequestBlocks::handle));
    }
    if calls.contains(&ConfigRpcCallJson::GetBlocksWithLimit) {
        processor.add_handler(
            "getBlocksWithLimit",
            Box::new(RpcRequestBlocksWithLimit::handle),
        );
    }
    if calls.contains(&ConfigRpcCallJson::GetBlockTime) {
        processor.add_handler("getBlockTime", Box::new(RpcRequestBlockTime::handle));
    }
    if calls.contains(&ConfigRpcCallJson::GetClusterNodes) {
        processor.add_handler("getClusterNodes", Box::new(RpcRequestClusterNodes::handle));
    }
    if calls.contains(&ConfigRpcCallJson::GetFirstAvailableBlock) {
        processor.add_handler(
            "getFirstAvailableBlock",
            Box::new(RpcRequestFirstAvailableBlock::handle),
        );
    }
    if calls.contains(&ConfigRpcCallJson::GetHealth) {
        processor.add_handler("getHealth", Box::new(RpcRequestHealth::handle));
        if let (Some(rpc_url), Some(cluster_slot)) =
            (config.health_check.rpc_uri.clone(), poller_slot)
        {
            let poll_interval = config.health_check.interval;
            tokio::spawn(async move {
                let sender = HttpSender::new(rpc_url);
                let client = RpcClient::new_sender(sender, RpcClientConfig::default());
                loop {
                    let value = match client
                        .get_slot_with_commitment(CommitmentConfig::processed())
                        .await
                    {
                        Ok(slot) => slot,
                        Err(_) => 0,
                    };
                    cluster_slot.store(value, Ordering::Relaxed);
                    sleep(poll_interval).await;
                }
            });
        }
    }
    if calls.contains(&ConfigRpcCallJson::GetInflationReward) {
        processor.add_handler(
            "getInflationReward",
            Box::new(RpcRequestInflationReward::handle),
        );
    }
    if calls.contains(&ConfigRpcCallJson::GetLatestBlockhash) {
        processor.add_handler(
            "getLatestBlockhash",
            Box::new(RpcRequestLatestBlockhash::handle),
        );
    }
    if calls.contains(&ConfigRpcCallJson::GetLeaderSchedule) {
        processor.add_handler(
            "getLeaderSchedule",
            Box::new(RpcRequestLeaderSchedule::handle),
        );
    }
    if calls.contains(&ConfigRpcCallJson::GetRecentPrioritizationFees) {
        processor.add_handler(
            "getRecentPrioritizationFees",
            Box::new(RpcRequestRecentPrioritizationFees::handle),
        );
    }
    if calls.contains(&ConfigRpcCallJson::GetSignaturesForAddress) {
        processor.add_handler(
            "getSignaturesForAddress",
            Box::new(RpcRequestSignaturesForAddress::handle),
        );
    }
    if calls.contains(&ConfigRpcCallJson::GetSignatureStatuses) {
        processor.add_handler(
            "getSignatureStatuses",
            Box::new(RpcRequestSignatureStatuses::handle),
        );
    }
    if calls.contains(&ConfigRpcCallJson::GetSlot) {
        processor.add_handler("getSlot", Box::new(RpcRequestSlot::handle));
    }
    if calls.contains(&ConfigRpcCallJson::GetTransaction) {
        processor.add_handler("getTransaction", Box::new(RpcRequestTransaction::handle));
    }
    if calls.contains(&ConfigRpcCallJson::GetTransactionsForAddress) {
        processor.add_handler(
            "getTransactionsForAddress",
            Box::new(RpcRequestTransactionsForAddress::handle),
        );
    }
    if calls.contains(&ConfigRpcCallJson::GetVersion) {
        processor.add_handler("getVersion", Box::new(RpcRequestVersion::handle));
    }
    if calls.contains(&ConfigRpcCallJson::IsBlockhashValid) {
        processor.add_handler(
            "isBlockhashValid",
            Box::new(RpcRequestIsBlockhashValid::handle),
        );
    }

    Ok(processor)
}

trait RpcRequestHandler: Sized {
    fn handle(
        state: Arc<State>,
        x_subscription_id: Arc<str>,
        upstream_disabled: bool,
        request: Request<'_>,
    ) -> BoxFuture<'_, RpcRequestResult>
    where
        Self: Send,
    {
        Box::pin(async move {
            match Self::parse(state, x_subscription_id, upstream_disabled, request) {
                Ok(req) => req.process().await,
                Err(response) => Ok(response),
            }
        })
    }

    fn parse(
        state: Arc<State>,
        x_subscription_id: Arc<str>,
        upstream_disabled: bool,
        request: Request<'_>,
    ) -> Result<Self, Vec<u8>>;

    fn process(self) -> impl Future<Output = RpcRequestResult> + Send {
        async { unimplemented!() }
    }
}

fn parse_params<'a, T>(request: Request<'a>) -> Result<(Id<'a>, T), Vec<u8>>
where
    T: for<'de> de::Deserialize<'de>,
{
    let params = Params::new(request.params.as_ref().map(|p| p.get()));
    match params.parse() {
        Ok(params) => Ok((request.id, params)),
        Err(error) => Err(to_vec(&Response {
            jsonrpc: Some(TwoPointZero),
            payload: ResponsePayload::<()>::error(error),
            id: request.id,
            extensions: Extensions::default(),
        })),
    }
}

fn no_params_expected(request: Request<'_>) -> Result<Request<'_>, Vec<u8>> {
    if let Some(error) = match serde_json::from_str::<serde_json::Value>(
        request.params.as_ref().map(|p| p.get()).unwrap_or("null"),
    ) {
        Ok(value) => match value {
            serde_json::Value::Null => None,
            serde_json::Value::Array(vec) if vec.is_empty() => None,
            value => Some(jsonrpc_error_invalid_params(
                "No parameters were expected",
                Some(value.to_string()),
            )),
        },
        Err(error) => Some(jsonrpc_error_invalid_params(
            INVALID_PARAMS_MSG,
            Some(error.to_string()),
        )),
    } {
        Err(jsonrpc_response_error(request.id, error))
    } else {
        Ok(request)
    }
}

fn check_is_at_least_confirmed(commitment: CommitmentConfig) -> Result<(), ErrorObjectOwned> {
    if !commitment.is_at_least_confirmed() {
        return Err(jsonrpc_error_invalid_params::<()>(
            "Method does not support commitment below `confirmed`",
            None,
        ));
    }
    Ok(())
}

fn min_context_check<'a>(
    id: Id<'a>,
    min_context_slot: Option<Slot>,
    commitment: CommitmentConfig,
    state: &State,
) -> Result<(Id<'a>, Option<Slot>), Vec<u8>> {
    if let Some(min_context_slot) = min_context_slot {
        let context_slot = match commitment.commitment {
            CommitmentLevel::Processed => state.stored_slots.processed_load(),
            CommitmentLevel::Confirmed => state.stored_slots.confirmed_load(),
            CommitmentLevel::Finalized => state.stored_slots.finalized_load(),
        };

        if context_slot < min_context_slot {
            Err(jsonrpc_response_error_custom(
                id,
                RpcCustomError::MinContextSlotNotReached { context_slot },
            ))
        } else {
            Ok((id, Some(context_slot)))
        }
    } else {
        Ok((id, None))
    }
}

fn verify_signature(input: &str) -> Result<Signature, ErrorObjectOwned> {
    input.parse().map_err(|error| {
        jsonrpc_error_invalid_params::<()>(format!("Invalid param: {error:?}"), None)
    })
}

fn verify_pubkey(input: &str) -> Result<Pubkey, ErrorObjectOwned> {
    input.parse().map_err(|error| {
        jsonrpc_error_invalid_params::<()>(format!("Invalid param: {error:?}"), None)
    })
}

fn verify_and_parse_signatures_for_address_params(
    address: String,
    before: Option<String>,
    until: Option<String>,
    limit: Option<usize>,
    default_limit: usize, // default is MAX_GET_CONFIRMED_SIGNATURES_FOR_ADDRESS2_LIMIT / 1000
) -> Result<(Pubkey, Option<Signature>, Option<Signature>, usize), ErrorObjectOwned> {
    let address = verify_pubkey(&address)?;
    let before = before
        .map(|ref before| verify_signature(before))
        .transpose()?;
    let until = until.map(|ref until| verify_signature(until)).transpose()?;
    let limit = limit.unwrap_or(default_limit);

    if limit == 0 || limit > default_limit {
        Err(jsonrpc_error_invalid_params::<()>(
            format!("Invalid limit; max {default_limit}"),
            None,
        ))
    } else {
        Ok((address, before, until, limit))
    }
}

async fn process_with_workers(
    (state, mut request, rx): (Arc<State>, WorkRequest, oneshot::Receiver<RpcRequestResult>),
) -> RpcRequestResult {
    loop {
        match state.workers.try_send(request) {
            Ok(()) => break,
            Err(TrySendError::Full(value)) => {
                request = value;
                sleep(Duration::from_micros(250)).await;
            }
            Err(TrySendError::Disconnected(_)) => anyhow::bail!("encode workers disconnected"),
        }
    }

    match rx.await {
        Ok(response) => response,
        Err(_) => anyhow::bail!("failed to get encoded request"),
    }
}

struct RpcRequestBlock {
    state: Arc<State>,
    x_subscription_id: Arc<str>,
    upstream_disabled: bool,
    id: Id<'static>,
    slot: Slot,
    commitment: CommitmentConfig,
    encoding: UiTransactionEncoding,
    encoding_options: BlockEncodingOptions,
}

impl RpcRequestHandler for RpcRequestBlock {
    fn parse(
        state: Arc<State>,
        x_subscription_id: Arc<str>,
        upstream_disabled: bool,
        request: Request<'_>,
    ) -> Result<Self, Vec<u8>> {
        #[derive(Debug, Deserialize)]
        struct ReqParams {
            slot: Slot,
            #[serde(default)]
            config: Option<RpcEncodingConfigWrapper<RpcBlockConfig>>,
        }

        let (id, ReqParams { slot, config }) = parse_params(request)?;

        let config = config
            .map(|config| config.convert_to_current())
            .unwrap_or_default();
        let encoding = config.encoding.unwrap_or(UiTransactionEncoding::Json);
        let encoding_options = BlockEncodingOptions {
            transaction_details: config.transaction_details.unwrap_or_default(),
            show_rewards: config.rewards.unwrap_or(true),
            max_supported_transaction_version: config.max_supported_transaction_version,
        };
        let commitment = config.commitment.unwrap_or_default();
        if let Err(error) = check_is_at_least_confirmed(commitment) {
            return Err(jsonrpc_response_error(id, error));
        }

        Ok(Self {
            state,
            x_subscription_id,
            upstream_disabled,
            id: id.into_owned(),
            slot,
            commitment,
            encoding,
            encoding_options,
        })
    }

    async fn process(self) -> RpcRequestResult {
        let deadline = Instant::now() + self.state.request_timeout;

        // check slot before sending request
        let slot_tip = match self.commitment.commitment {
            CommitmentLevel::Processed => unreachable!(),
            CommitmentLevel::Confirmed => self.state.stored_slots.confirmed_load(),
            CommitmentLevel::Finalized => self.state.stored_slots.finalized_load(),
        };
        if self.slot > slot_tip {
            return Ok(Self::error_not_available(self.id, self.slot));
        }
        if self.slot <= self.state.stored_slots.first_available_load() {
            return self.fetch_upstream(deadline, REASON_BELOW_RETENTION).await;
        }

        // request
        let (tx, rx) = oneshot::channel();
        anyhow::ensure!(
            self.state
                .requests_tx
                .send(ReadRequest::Block {
                    deadline,
                    slot: self.slot,
                    tx,
                    x_subscription_id: Arc::clone(&self.x_subscription_id),
                })
                .await
                .is_ok(),
            "request channel is closed"
        );
        let Ok(result) = rx.await else {
            anyhow::bail!("rx channel is closed");
        };
        let bytes = match result {
            ReadResultBlock::Timeout => anyhow::bail!("timeout"),
            ReadResultBlock::Removed => {
                return self.fetch_upstream(deadline, REASON_REMOVED).await;
            }
            ReadResultBlock::Dead => {
                return Ok(Self::error_skipped(self.id, self.slot));
            }
            ReadResultBlock::NotAvailable => {
                return Ok(Self::error_not_available(self.id, self.slot));
            }
            ReadResultBlock::Block(bytes) => bytes,
            ReadResultBlock::ReadError(error) => anyhow::bail!("read error: {error}"),
        };

        // verify that we still have data for that block (i.e. we read correct data)
        if self.slot <= self.state.stored_slots.first_available_load() {
            return self.fetch_upstream(deadline, REASON_BELOW_RETENTION).await;
        }

        // parse, encode and serialize
        process_with_workers(RpcRequestBlockWorkRequest::create(self, bytes)).await
    }
}

impl RpcRequestBlock {
    async fn fetch_upstream(self, deadline: Instant, reason: &'static str) -> RpcRequestResult {
        if self.upstream_disabled {
            return Ok(jsonrpc_response_success(self.id, None::<()>));
        }

        if let Some(upstream) = self.state.get_upstream(ConfigRpcCallJson::GetBlock) {
            debug!(self.slot, reason = ?reason, "getBlock slot going upstream");
            upstream
                .get_block(
                    self.x_subscription_id,
                    deadline,
                    &self.id,
                    self.slot,
                    self.commitment,
                    self.encoding,
                    self.encoding_options,
                    reason,
                )
                .await
        } else {
            Ok(Self::error_skipped_long_term_storage(self.id, self.slot))
        }
    }

    fn error_not_available(id: Id<'static>, slot: Slot) -> Vec<u8> {
        jsonrpc_response_error_custom(id, RpcCustomError::BlockNotAvailable { slot })
    }

    fn error_skipped(id: Id<'static>, slot: Slot) -> Vec<u8> {
        jsonrpc_response_error_custom(id, RpcCustomError::SlotSkipped { slot })
    }

    fn error_skipped_long_term_storage(id: Id<'static>, slot: Slot) -> Vec<u8> {
        jsonrpc_response_error_custom(id, RpcCustomError::LongTermStorageSlotSkipped { slot })
    }
}

pub struct RpcRequestBlockWorkRequest {
    x_subscription_id: Arc<str>,
    id: Id<'static>,
    slot: Slot,
    encoding: UiTransactionEncoding,
    encoding_options: BlockEncodingOptions,
    bytes: Vec<u8>,
    tx: Option<oneshot::Sender<RpcRequestResult>>,
}

impl RpcRequestBlockWorkRequest {
    fn create(
        request: RpcRequestBlock,
        bytes: Vec<u8>,
    ) -> (Arc<State>, WorkRequest, oneshot::Receiver<RpcRequestResult>) {
        let (tx, rx) = oneshot::channel();
        let this = Self {
            x_subscription_id: request.x_subscription_id,
            id: request.id,
            slot: request.slot,
            encoding: request.encoding,
            encoding_options: request.encoding_options,
            bytes,
            tx: Some(tx),
        };
        (request.state, WorkRequest::Block(this), rx)
    }

    pub fn process(mut self) {
        if let Some(tx) = self.tx.take() {
            let ts = quanta::Instant::now();
            let _ = tx.send(Self::process2(
                self.bytes,
                self.id,
                self.slot,
                self.encoding,
                self.encoding_options,
            ));
            gauge!(
                RPC_WORKERS_CPU_SECONDS_TOTAL,
                "x_subscription_id" => self.x_subscription_id,
                "method" => "getBlock"
            )
            .increment(duration_to_seconds(ts.elapsed()));
        }
    }

    fn process2(
        bytes: Vec<u8>,
        id: Id<'static>,
        slot: Slot,
        encoding: UiTransactionEncoding,
        encoding_options: BlockEncodingOptions,
    ) -> RpcRequestResult {
        // parse, encode and serialize
        Ok(
            match Self::parse_and_encode(&bytes, &id, slot, encoding, encoding_options)? {
                Ok(block) => jsonrpc_response_success(id, &block),
                Err(error) => error,
            },
        )
    }

    fn parse_and_encode(
        bytes: &[u8],
        id: &Id<'static>,
        slot: Slot,
        encoding: UiTransactionEncoding,
        encoding_options: BlockEncodingOptions,
    ) -> anyhow::Result<Result<UiConfirmedBlock, Vec<u8>>> {
        // parse
        let block = match generated::ConfirmedBlock::decode(bytes) {
            Ok(block) => match ConfirmedBlock::try_from(block) {
                Ok(block) => block,
                Err(error) => {
                    error!(slot, ?error, "failed to decode block");
                    anyhow::bail!("failed to decode block")
                }
            },
            Err(error) => {
                error!(slot, ?error, "failed to decode block protobuf / bincode");
                anyhow::bail!("failed to decode block protobuf / bincode")
            }
        };

        // encode
        let result = match block.encode_with_options(encoding, encoding_options) {
            Ok(block) => Ok(block),
            Err(error) => Err(jsonrpc_response_error_custom(
                id.clone(),
                RpcCustomError::from(error),
            )),
        };
        Ok(result)
    }
}

#[derive(Debug)]
struct RpcRequestBlockHeight {
    state: Arc<State>,
    id: Id<'static>,
    commitment: CommitmentConfig,
}

impl RpcRequestHandler for RpcRequestBlockHeight {
    fn parse(
        state: Arc<State>,
        _x_subscription_id: Arc<str>,
        _upstream_disabled: bool,
        request: Request<'_>,
    ) -> Result<Self, Vec<u8>> {
        #[derive(Debug, Default, Deserialize)]
        struct ReqParams {
            #[serde(default)]
            config: Option<RpcContextConfig>,
        }

        let (id, ReqParams { config }) = if request.params.is_some() {
            parse_params(request)?
        } else {
            (request.id, Default::default())
        };
        let RpcContextConfig {
            commitment,
            min_context_slot,
        } = config.unwrap_or_default();
        let commitment = commitment.unwrap_or_default();

        let (id, _slot) = min_context_check(id, min_context_slot, commitment, &state)?;

        Ok(Self {
            state,
            id: id.into_owned(),
            commitment,
        })
    }

    async fn process(self) -> RpcRequestResult {
        let deadline = Instant::now() + self.state.request_timeout;

        // request
        let (tx, rx) = oneshot::channel();
        anyhow::ensure!(
            self.state
                .requests_tx
                .send(ReadRequest::BlockHeight {
                    deadline,
                    commitment: self.commitment,
                    tx
                })
                .await
                .is_ok(),
            "request channel is closed"
        );
        let Ok(result) = rx.await else {
            anyhow::bail!("rx channel is closed");
        };
        let block_height = match result {
            ReadResultBlockHeight::Timeout => anyhow::bail!("timeout"),
            ReadResultBlockHeight::BlockHeight { block_height, .. } => block_height,
            ReadResultBlockHeight::ReadError(error) => anyhow::bail!("read error: {error}"),
        };
        Ok(jsonrpc_response_success(self.id, json!(block_height)))
    }
}

#[derive(Debug, Clone, Copy)]
pub enum RpcRequestBlocksUntil {
    EndSlot(Slot),
    Limit(usize),
}

#[derive(Debug)]
struct RpcRequestBlocks {
    state: Arc<State>,
    x_subscription_id: Arc<str>,
    upstream_disabled: bool,
    id: Id<'static>,
    start_slot: Slot,
    until: RpcRequestBlocksUntil,
    commitment: CommitmentConfig,
}

impl RpcRequestHandler for RpcRequestBlocks {
    fn parse(
        state: Arc<State>,
        x_subscription_id: Arc<str>,
        upstream_disabled: bool,
        request: Request<'_>,
    ) -> Result<Self, Vec<u8>> {
        #[derive(Debug, Deserialize)]
        struct ReqParams {
            start_slot: Slot,
            #[serde(default)]
            wrapper: Option<RpcBlocksConfigWrapper>,
            #[serde(default)]
            config: Option<RpcContextConfig>,
        }

        let (
            id,
            ReqParams {
                start_slot,
                wrapper,
                config,
            },
        ) = parse_params(request)?;
        let (end_slot, maybe_config) = wrapper.map(|wrapper| wrapper.unzip()).unwrap_or_default();
        let config = config.or(maybe_config).unwrap_or_default();

        let commitment = config.commitment.unwrap_or_default();
        if let Err(error) = check_is_at_least_confirmed(commitment) {
            return Err(jsonrpc_response_error(id, error));
        }

        let min_context_slot = config.min_context_slot.unwrap_or_default();
        let finalized_slot = state.stored_slots.finalized_load();
        if commitment.is_finalized() && finalized_slot < min_context_slot {
            return Err(jsonrpc_response_error_custom(
                id,
                RpcCustomError::MinContextSlotNotReached {
                    context_slot: finalized_slot,
                },
            ));
        }

        let end_slot = std::cmp::min(
            end_slot.unwrap_or_else(|| start_slot.saturating_add(MAX_GET_CONFIRMED_BLOCKS_RANGE)),
            if commitment.is_finalized() {
                finalized_slot
            } else {
                state.stored_slots.confirmed_load()
            },
        );
        if end_slot < start_slot {
            return Err(jsonrpc_response_success(id, json!([])));
        }
        if end_slot - start_slot > MAX_GET_CONFIRMED_BLOCKS_RANGE {
            return Err(jsonrpc_response_error(
                id,
                jsonrpc_error_invalid_params::<()>(
                    format!("Slot range too large; max {MAX_GET_CONFIRMED_BLOCKS_RANGE}"),
                    None,
                ),
            ));
        }

        Ok(Self {
            state,
            x_subscription_id,
            upstream_disabled,
            id: id.into_owned(),
            start_slot,
            until: RpcRequestBlocksUntil::EndSlot(end_slot),
            commitment,
        })
    }

    async fn process(self) -> RpcRequestResult {
        let deadline = Instant::now() + self.state.request_timeout;

        // some slot will be removed while we pass request, send to upstream
        let first_available_slot = self.state.stored_slots.first_available_load() + 32;
        if self.start_slot < first_available_slot
            && let Some(upstream) = (!self.upstream_disabled)
                .then(|| self.state.get_upstream(ConfigRpcCallJson::GetBlocks))
                .flatten()
        {
            return upstream
                .get_blocks(
                    Arc::clone(&self.x_subscription_id),
                    deadline,
                    &self.id,
                    self.start_slot,
                    self.until,
                    self.commitment,
                    REASON_BELOW_RETENTION,
                )
                .await;
        }

        // request
        let (tx, rx) = oneshot::channel();
        anyhow::ensure!(
            self.state
                .requests_tx
                .send(ReadRequest::Blocks {
                    deadline,
                    start_slot: self.start_slot,
                    until: self.until,
                    commitment: self.commitment,
                    tx
                })
                .await
                .is_ok(),
            "request channel is closed"
        );
        let Ok(result) = rx.await else {
            anyhow::bail!("rx channel is closed");
        };

        match result {
            ReadResultBlocks::Timeout => anyhow::bail!("timeout"),
            ReadResultBlocks::Blocks(blocks) => Ok(jsonrpc_response_success(self.id, &blocks)),
            ReadResultBlocks::Removed => Ok(jsonrpc_response_error_custom(
                self.id,
                RpcCustomError::BlockCleanedUp {
                    slot: self.start_slot,
                    first_available_block: self.state.stored_slots.first_available_load(),
                },
            )),
            ReadResultBlocks::ReadError(error) => anyhow::bail!("read error: {error}"),
        }
    }
}

#[derive(Debug)]
struct RpcRequestBlocksWithLimit {
    state: Arc<State>,
    x_subscription_id: Arc<str>,
    upstream_disabled: bool,
    id: Id<'static>,
    start_slot: Slot,
    until: RpcRequestBlocksUntil,
    commitment: CommitmentConfig,
}

impl RpcRequestHandler for RpcRequestBlocksWithLimit {
    fn parse(
        state: Arc<State>,
        x_subscription_id: Arc<str>,
        upstream_disabled: bool,
        request: Request<'_>,
    ) -> Result<Self, Vec<u8>> {
        #[derive(Debug, Deserialize)]
        struct ReqParams {
            start_slot: Slot,
            limit: usize,
            #[serde(default)]
            config: Option<RpcContextConfig>,
        }

        let (
            id,
            ReqParams {
                start_slot,
                limit,
                config,
            },
        ) = parse_params(request)?;
        let config = config.unwrap_or_default();

        let commitment = config.commitment.unwrap_or_default();
        if let Err(error) = check_is_at_least_confirmed(commitment) {
            return Err(jsonrpc_response_error(id, error));
        }

        let min_context_slot = config.min_context_slot.unwrap_or_default();
        let finalized_slot = state.stored_slots.finalized_load();
        if commitment.is_finalized() && finalized_slot < min_context_slot {
            return Err(jsonrpc_response_error_custom(
                id,
                RpcCustomError::MinContextSlotNotReached {
                    context_slot: finalized_slot,
                },
            ));
        }

        if limit == 0 {
            return Err(jsonrpc_response_success(id, json!([])));
        }
        if limit > MAX_GET_CONFIRMED_BLOCKS_RANGE as usize {
            return Err(jsonrpc_response_error(
                id,
                jsonrpc_error_invalid_params::<()>(
                    format!("Limit too large; max {MAX_GET_CONFIRMED_BLOCKS_RANGE}"),
                    None,
                ),
            ));
        }

        Ok(Self {
            state,
            x_subscription_id,
            upstream_disabled,
            id: id.into_owned(),
            start_slot,
            until: RpcRequestBlocksUntil::Limit(limit),
            commitment,
        })
    }

    async fn process(self) -> RpcRequestResult {
        RpcRequestBlocks {
            state: self.state,
            x_subscription_id: self.x_subscription_id,
            upstream_disabled: self.upstream_disabled,
            id: self.id,
            start_slot: self.start_slot,
            until: self.until,
            commitment: self.commitment,
        }
        .process()
        .await
    }
}

#[derive(Debug)]
struct RpcRequestBlockTime {
    state: Arc<State>,
    x_subscription_id: Arc<str>,
    upstream_disabled: bool,
    id: Id<'static>,
    slot: Slot,
}

impl RpcRequestHandler for RpcRequestBlockTime {
    fn parse(
        state: Arc<State>,
        x_subscription_id: Arc<str>,
        upstream_disabled: bool,
        request: Request<'_>,
    ) -> Result<Self, Vec<u8>> {
        #[derive(Debug, Deserialize)]
        struct ReqParams {
            slot: Slot,
        }

        let (id, ReqParams { slot }) = parse_params(request)?;

        if slot == 0 {
            Err(jsonrpc_response_success(id, 1584368940))
        } else {
            Ok(Self {
                state,
                x_subscription_id,
                upstream_disabled,
                id: id.into_owned(),
                slot,
            })
        }
    }

    async fn process(self) -> RpcRequestResult {
        let deadline = Instant::now() + self.state.request_timeout;

        // request
        let (tx, rx) = oneshot::channel();
        anyhow::ensure!(
            self.state
                .requests_tx
                .send(ReadRequest::BlockTime {
                    deadline,
                    slot: self.slot,
                    tx
                })
                .await
                .is_ok(),
            "request channel is closed"
        );
        let Ok(result) = rx.await else {
            anyhow::bail!("rx channel is closed");
        };
        let response = match result {
            ReadResultBlockTime::Timeout => anyhow::bail!("timeout"),
            ReadResultBlockTime::Removed => {
                return self.fetch_upstream(deadline, REASON_REMOVED).await;
            }
            ReadResultBlockTime::Dead => Err(RpcCustomError::SlotSkipped { slot: self.slot }),
            ReadResultBlockTime::NotAvailable => {
                Err(RpcCustomError::BlockNotAvailable { slot: self.slot })
            }
            ReadResultBlockTime::BlockTime(block_time) => Ok(block_time),
            ReadResultBlockTime::ReadError(error) => anyhow::bail!("read error: {error}"),
        };

        Ok(match response {
            Ok(payload) => jsonrpc_response_success(self.id, payload),
            Err(error) => jsonrpc_response_error_custom(self.id, error),
        })
    }
}

impl RpcRequestBlockTime {
    async fn fetch_upstream(self, deadline: Instant, reason: &'static str) -> RpcRequestResult {
        if self.upstream_disabled {
            return Ok(Self::error_cleaned_up(
                self.id,
                self.slot,
                self.state.stored_slots.first_available_load(),
            ));
        }

        if let Some(upstream) = self.state.get_upstream(ConfigRpcCallJson::GetBlockTime) {
            upstream
                .get_block_time(
                    self.x_subscription_id,
                    deadline,
                    &self.id,
                    self.slot,
                    reason,
                )
                .await
        } else {
            Ok(Self::error_cleaned_up(
                self.id,
                self.slot,
                self.state.stored_slots.first_available_load(),
            ))
        }
    }

    fn error_cleaned_up(id: Id<'static>, slot: Slot, first_available_block: Slot) -> Vec<u8> {
        jsonrpc_response_error_custom(
            id,
            RpcCustomError::BlockCleanedUp {
                slot,
                first_available_block,
            },
        )
    }
}

#[derive(Debug)]
struct RpcRequestClusterNodes {
    state: Arc<State>,
    x_subscription_id: Arc<str>,
    id: Id<'static>,
}

impl RpcRequestHandler for RpcRequestClusterNodes {
    fn parse(
        state: Arc<State>,
        x_subscription_id: Arc<str>,
        _upstream_disabled: bool,
        request: Request<'_>,
    ) -> Result<Self, Vec<u8>> {
        let request = no_params_expected(request)?;
        Ok(Self {
            state,
            x_subscription_id,
            id: request.id.into_owned(),
        })
    }

    async fn process(self) -> RpcRequestResult {
        let deadline = Instant::now() + self.state.request_timeout;

        let Some(upstream) = self.state.get_upstream(ConfigRpcCallJson::GetClusterNodes) else {
            unreachable!();
        };

        upstream
            .get_cluster_nodes(self.x_subscription_id, deadline, self.id, REASON_ALWAYS)
            .await
    }
}

#[derive(Debug)]
struct RpcRequestFirstAvailableBlock {
    state: Arc<State>,
    x_subscription_id: Arc<str>,
    upstream_disabled: bool,
    id: Id<'static>,
}

impl RpcRequestHandler for RpcRequestFirstAvailableBlock {
    fn parse(
        state: Arc<State>,
        x_subscription_id: Arc<str>,
        upstream_disabled: bool,
        request: Request<'_>,
    ) -> Result<Self, Vec<u8>> {
        let request = no_params_expected(request)?;
        Ok(Self {
            state,
            x_subscription_id,
            upstream_disabled,
            id: request.id.into_owned(),
        })
    }

    async fn process(self) -> RpcRequestResult {
        let deadline = Instant::now() + self.state.request_timeout;

        if let Some(upstream) = (!self.upstream_disabled)
            .then(|| {
                self.state
                    .get_upstream(ConfigRpcCallJson::GetFirstAvailableBlock)
            })
            .flatten()
        {
            upstream
                .get_first_available_block(
                    self.x_subscription_id,
                    deadline,
                    &self.id,
                    REASON_ALWAYS,
                )
                .await
        } else {
            Ok(jsonrpc_response_success(
                self.id,
                json!(self.state.stored_slots.first_available_load()),
            ))
        }
    }
}

#[derive(Debug)]
struct RpcRequestInflationReward {
    state: Arc<State>,
    x_subscription_id: Arc<str>,
    upstream_disabled: bool,
    id: Id<'static>,
    commitment: CommitmentConfig,
    epoch: Epoch,
    addresses: Vec<Pubkey>,
}

impl RpcRequestHandler for RpcRequestInflationReward {
    fn parse(
        state: Arc<State>,
        x_subscription_id: Arc<str>,
        upstream_disabled: bool,
        request: Request<'_>,
    ) -> Result<Self, Vec<u8>> {
        #[derive(Debug, Default, Deserialize)]
        struct ReqParams {
            #[serde(default)]
            address_strs: Vec<String>,
            #[serde(default)]
            config: Option<RpcEpochConfig>,
        }

        let (
            id,
            ReqParams {
                address_strs,
                config,
            },
        ) = if request.params.is_some() {
            parse_params(request)?
        } else {
            (request.id, Default::default())
        };
        let mut addresses = Vec::with_capacity(address_strs.len());
        for address_str in address_strs.iter() {
            match verify_pubkey(address_str) {
                Ok(pubkey) => addresses.push(pubkey),
                Err(error) => return Err(jsonrpc_response_error(id, error)),
            }
        }
        let RpcEpochConfig {
            epoch,
            commitment,
            min_context_slot,
        } = config.unwrap_or_default();
        let commitment = commitment.unwrap_or_default();
        let (id, epoch) = match epoch {
            Some(epoch) => (id, epoch),
            None => {
                let (id, slot) = min_context_check(id, min_context_slot, commitment, &state)?;
                let slot = slot.unwrap_or_else(|| match commitment.commitment {
                    CommitmentLevel::Processed => state.stored_slots.processed_load(),
                    CommitmentLevel::Confirmed => state.stored_slots.confirmed_load(),
                    CommitmentLevel::Finalized => state.stored_slots.finalized_load(),
                });
                (id, state.epoch_schedule.get_epoch(slot).saturating_sub(1))
            }
        };

        Ok(Self {
            state,
            x_subscription_id,
            upstream_disabled,
            id: id.into_owned(),
            commitment,
            epoch,
            addresses,
        })
    }

    async fn process(mut self) -> RpcRequestResult {
        let deadline = Instant::now() + self.state.request_timeout;

        // request
        let (tx, rx) = oneshot::channel();
        anyhow::ensure!(
            self.state
                .requests_tx
                .send(ReadRequest::InflationReward {
                    deadline,
                    epoch: self.epoch,
                    addresses: std::mem::take(&mut self.addresses),
                    tx,
                    x_subscription_id: Arc::clone(&self.x_subscription_id),
                })
                .await
                .is_ok(),
            "request channel is closed"
        );
        let Ok(result) = rx.await else {
            anyhow::bail!("rx channel is closed");
        };

        let rewards = match result {
            ReadResultInflationReward::Timeout => anyhow::bail!("timeout"),
            ReadResultInflationReward::Reward(ReadRequestResultInflationReward {
                addresses,
                mut rewards,
                missed,
                base,
            }) => {
                if !missed.is_empty()
                    && let Err(error) = self
                        .get_inflation_reward(deadline, addresses, &mut rewards, missed, base)
                        .await?
                {
                    return Ok(error);
                }
                rewards
            }
            ReadResultInflationReward::ReadError(error) => anyhow::bail!("read error: {error}"),
        };

        // serialize
        Ok(jsonrpc_response_success(self.id, &rewards))
    }
}

impl RpcRequestInflationReward {
    async fn get_inflation_reward(
        &mut self,
        deadline: Instant,
        addresses: Vec<Pubkey>,
        rewards: &mut [Option<RpcInflationReward>],
        mut missed: Vec<usize>,
        base: Option<InflationRewardBaseValue>,
    ) -> anyhow::Result<Result<(), Vec<u8>>> {
        let epoch_boundary_block = match base {
            Some(base) => base,
            None => match self
                .get_inflation_reward_base(deadline, &addresses, rewards, &mut missed)
                .await?
            {
                Ok(base) => {
                    if missed.is_empty() {
                        return Ok(Ok(()));
                    }
                    base
                }
                Err(error) => return Ok(Err(error)),
            },
        };

        // append stake account rewards from partitions
        if let Some(num_partitions) = epoch_boundary_block.num_reward_partitions {
            let num_partitions = usize::try_from(num_partitions)
                .expect("num_partitions should never exceed usize::MAX");

            // fetch blocks with rewards
            let block_list = match self
                .get_blocks_with_limit(deadline, epoch_boundary_block.slot + 1, num_partitions)
                .await?
            {
                Ok(blocks) => blocks,
                Err(error) => return Ok(Err(error)),
            };

            // calculate partitions for addresses
            let hasher =
                EpochRewardsHasher::new(num_partitions, &epoch_boundary_block.previous_blockhash);
            let mut partition_index_addresses: HashMap<usize, Vec<usize>> = HashMap::default();
            for index in missed {
                let partition_index = hasher.clone().hash_address_to_partition(&addresses[index]);
                partition_index_addresses
                    .entry(partition_index)
                    .or_insert_with(|| Vec::with_capacity(4))
                    .push(index);
            }

            // fetch blocks with rewards
            for (partition_index, missed) in partition_index_addresses {
                let Some(slot) = block_list.get(partition_index).copied() else {
                    // If block_list.len() too short to contain
                    // partition_index, the epoch rewards period must be
                    // currently active.
                    let rewards_complete_block_height = epoch_boundary_block
                        .block_height
                        .map(|block_height| {
                            block_height
                                .saturating_add(num_partitions as u64)
                                .saturating_add(1)
                        })
                        .expect(
                            "every block after partitioned_epoch_reward_enabled should have a \
                                populated block_height",
                        );

                    return self
                        .error_epoch_rewards_period_active(deadline, rewards_complete_block_height)
                        .await
                        .map(Err);
                };

                // fetch block
                let block = match self.get_block_with_rewards(deadline, slot).await? {
                    Ok(block) => block,
                    Err(error) => return Ok(Err(error)),
                };

                // collect for addresses
                let parititon_reward_map =
                    self.filter_rewards(slot, block.rewards, &|reward_type| {
                        reward_type == RewardType::Staking
                            || reward_type == RewardType::DeactivatedStake
                    })?;
                for index in missed {
                    if let Some(reward) = parititon_reward_map.get(&addresses[index]) {
                        rewards[index] = Some(reward.clone());
                    }
                }

                // send to writer
                self.state.db_write_inflation_reward.push_partition(
                    self.epoch,
                    partition_index,
                    parititon_reward_map,
                );
            }
        }

        Ok(Ok(()))
    }

    async fn get_inflation_reward_base(
        &self,
        deadline: Instant,
        addresses: &[Pubkey],
        rewards: &mut [Option<RpcInflationReward>],
        missed: &mut Vec<usize>,
    ) -> anyhow::Result<Result<InflationRewardBaseValue, Vec<u8>>> {
        // get first slot in epoch
        let first_slot_in_epoch = self
            .state
            .epoch_schedule
            .get_first_slot_in_epoch(self.epoch.saturating_add(1));
        if first_slot_in_epoch > self.state.stored_slots.confirmed_load() {
            return Ok(Err(RpcRequestBlock::error_not_available(
                self.id.clone(),
                first_slot_in_epoch,
            )));
        }
        let first_confirmed_block_in_epoch = match self
            .get_blocks_with_limit(deadline, first_slot_in_epoch, 1)
            .await?
            .map(|vec| vec.first().copied())
        {
            Ok(Some(slot)) => slot,
            Ok(None) => {
                return Ok(Err(RpcRequestBlock::error_not_available(
                    self.id.clone(),
                    first_slot_in_epoch,
                )));
            }
            Err(error) => return Ok(Err(error)),
        };
        let epoch_boundary_block = match self
            .get_block_with_rewards(deadline, first_confirmed_block_in_epoch)
            .await?
        {
            Ok(block) => block,
            Err(error) => return Ok(Err(error)),
        };
        let previous_blockhash = Hash::from_str(&epoch_boundary_block.previous_blockhash)
            .expect("UiConfirmedBlock::previous_blockhash should be properly formed");

        // collect rewards from epoch boundary slot
        let epoch_has_partitioned_rewards = epoch_boundary_block.num_reward_partitions.is_some();
        let epoch_reward_map = self.filter_rewards(
            first_confirmed_block_in_epoch,
            epoch_boundary_block.rewards,
            &|reward_type| {
                reward_type == RewardType::Voting
                    || (!epoch_has_partitioned_rewards && reward_type == RewardType::Staking)
            },
        )?;
        missed.retain(|index| {
            if let Some(reward) = epoch_reward_map.get(&addresses[*index]) {
                rewards[*index] = Some(reward.clone());
                false
            } else {
                true
            }
        });

        // send to writer
        self.state.db_write_inflation_reward.push_base(
            self.epoch,
            first_confirmed_block_in_epoch,
            epoch_boundary_block.block_height,
            previous_blockhash,
            epoch_boundary_block.num_reward_partitions,
            epoch_reward_map,
        );

        Ok(Ok(InflationRewardBaseValue::new(
            first_confirmed_block_in_epoch,
            epoch_boundary_block.block_height,
            previous_blockhash,
            epoch_boundary_block.num_reward_partitions,
        )))
    }

    async fn get_blocks_with_limit(
        &self,
        deadline: Instant,
        start_slot: Slot,
        limit: usize,
    ) -> anyhow::Result<Result<Vec<Slot>, Vec<u8>>> {
        // some slot will be removed while we pass request, send to upstream
        let first_available_slot = self.state.stored_slots.first_available_load() + 150;
        if start_slot < first_available_slot
            && let Some(upstream) = (!self.upstream_disabled)
                .then(|| {
                    self.state
                        .get_upstream(ConfigRpcCallJson::GetBlocksWithLimit)
                })
                .flatten()
        {
            return upstream
                .get_blocks_parsed(
                    Arc::clone(&self.x_subscription_id),
                    deadline,
                    &self.id,
                    start_slot,
                    limit,
                    REASON_BELOW_RETENTION,
                )
                .await
                .map_err(|error| anyhow::anyhow!(error));
        }

        // request
        let (tx, rx) = oneshot::channel();
        anyhow::ensure!(
            self.state
                .requests_tx
                .send(ReadRequest::Blocks {
                    deadline,
                    start_slot,
                    until: RpcRequestBlocksUntil::Limit(limit),
                    commitment: CommitmentConfig::confirmed(),
                    tx
                })
                .await
                .is_ok(),
            "request channel is closed"
        );
        let Ok(result) = rx.await else {
            anyhow::bail!("rx channel is closed");
        };

        match result {
            ReadResultBlocks::Timeout => anyhow::bail!("timeout"),
            ReadResultBlocks::Blocks(blocks) => Ok(Ok(blocks)),
            ReadResultBlocks::Removed => Ok(Err(jsonrpc_response_error_custom(
                self.id.clone(),
                RpcCustomError::BlockCleanedUp {
                    slot: start_slot,
                    first_available_block: self.state.stored_slots.first_available_load(),
                },
            ))),
            ReadResultBlocks::ReadError(error) => anyhow::bail!("read error: {error}"),
        }
    }

    async fn get_block_with_rewards(
        &self,
        deadline: Instant,
        slot: Slot,
    ) -> anyhow::Result<Result<UiConfirmedBlock, Vec<u8>>> {
        if slot <= self.state.stored_slots.first_available_load() {
            return self
                .get_block_with_rewards_upstream(deadline, slot, REASON_BELOW_RETENTION)
                .await;
        }

        // request
        let (tx, rx) = oneshot::channel();
        anyhow::ensure!(
            self.state
                .requests_tx
                .send(ReadRequest::Block {
                    deadline,
                    slot,
                    tx,
                    x_subscription_id: Arc::clone(&self.x_subscription_id),
                })
                .await
                .is_ok(),
            "request channel is closed"
        );
        let Ok(result) = rx.await else {
            anyhow::bail!("rx channel is closed");
        };
        let bytes = match result {
            ReadResultBlock::Timeout => anyhow::bail!("timeout"),
            ReadResultBlock::Removed => {
                return self
                    .get_block_with_rewards_upstream(deadline, slot, REASON_REMOVED)
                    .await;
            }
            ReadResultBlock::Dead => {
                return Ok(Err(RpcRequestBlock::error_skipped(self.id.clone(), slot)));
            }
            ReadResultBlock::NotAvailable => {
                return Ok(Err(RpcRequestBlock::error_not_available(
                    self.id.clone(),
                    slot,
                )));
            }
            ReadResultBlock::Block(bytes) => bytes,
            ReadResultBlock::ReadError(error) => anyhow::bail!("read error: {error}"),
        };

        // verify that we still have data for that block (i.e. we read correct data)
        if slot <= self.state.stored_slots.first_available_load() {
            return self
                .get_block_with_rewards_upstream(deadline, slot, REASON_BELOW_RETENTION)
                .await;
        }

        // parse and encode
        RpcRequestBlockWorkRequest::parse_and_encode(
            &bytes,
            &self.id,
            slot,
            UiTransactionEncoding::Base58,
            BlockEncodingOptions {
                transaction_details: TransactionDetails::None,
                show_rewards: true,
                max_supported_transaction_version: None,
            },
        )
    }

    async fn get_block_with_rewards_upstream(
        &self,
        deadline: Instant,
        slot: Slot,
        reason: &'static str,
    ) -> anyhow::Result<Result<UiConfirmedBlock, Vec<u8>>> {
        if let Some(upstream) = (!self.upstream_disabled)
            .then(|| self.state.get_upstream(ConfigRpcCallJson::GetBlock))
            .flatten()
        {
            match upstream
                .get_block_rewards(
                    Arc::clone(&self.x_subscription_id),
                    deadline,
                    &self.id,
                    slot,
                    reason,
                )
                .await
            {
                Ok(Ok(Some(block))) => Ok(Ok(block)),
                Ok(Ok(None)) => Ok(Err(RpcRequestBlock::error_not_available(
                    self.id.clone(),
                    slot,
                ))),
                Ok(Err(error)) => Ok(Err(error)),
                Err(error) => anyhow::bail!(error),
            }
        } else {
            Ok(Err(RpcRequestBlock::error_skipped_long_term_storage(
                self.id.clone(),
                slot,
            )))
        }
    }

    fn filter_rewards<F>(
        &self,
        slot: Slot,
        rewards: Option<Vec<Reward>>,
        reward_type_filter: &F,
    ) -> anyhow::Result<HashMap<Pubkey, RpcInflationReward>>
    where
        F: Fn(RewardType) -> bool,
    {
        rewards
            .into_iter()
            .flatten()
            .filter_map(move |reward| {
                reward.reward_type.is_some_and(reward_type_filter).then(|| {
                    let pubkey = reward
                        .pubkey
                        .parse()
                        .map_err(|_| anyhow::anyhow!("failed to parse {}", reward.pubkey))?;
                    let inflation_reward = RpcInflationReward {
                        epoch: self.epoch,
                        effective_slot: slot,
                        amount: reward.lamports.unsigned_abs(),
                        post_balance: reward.post_balance,
                        commission: reward.commission,
                        commission_bps: reward.commission_bps,
                    };
                    Ok((pubkey, inflation_reward))
                })
            })
            .collect()
    }

    async fn error_epoch_rewards_period_active(
        &self,
        deadline: Instant,
        rewards_complete_block_height: Slot,
    ) -> RpcRequestResult {
        // request
        let (tx, rx) = oneshot::channel();
        anyhow::ensure!(
            self.state
                .requests_tx
                .send(ReadRequest::BlockHeight {
                    deadline,
                    commitment: self.commitment,
                    tx
                })
                .await
                .is_ok(),
            "request channel is closed"
        );
        let Ok(result) = rx.await else {
            anyhow::bail!("rx channel is closed");
        };
        match result {
            ReadResultBlockHeight::Timeout => anyhow::bail!("timeout"),
            ReadResultBlockHeight::BlockHeight { block_height, slot } => {
                Ok(jsonrpc_response_error_custom(
                    self.id.clone(),
                    RpcCustomError::EpochRewardsPeriodActive {
                        slot,
                        current_block_height: block_height,
                        rewards_complete_block_height,
                    },
                ))
            }
            ReadResultBlockHeight::ReadError(error) => anyhow::bail!("read error: {error}"),
        }
    }
}

#[derive(Debug)]
struct RpcRequestLatestBlockhash {
    state: Arc<State>,
    id: Id<'static>,
    commitment: CommitmentConfig,
}

impl RpcRequestHandler for RpcRequestLatestBlockhash {
    fn parse(
        state: Arc<State>,
        _x_subscription_id: Arc<str>,
        _upstream_disabled: bool,
        request: Request<'_>,
    ) -> Result<Self, Vec<u8>> {
        #[derive(Debug, Default, Deserialize)]
        struct ReqParams {
            #[serde(default)]
            config: Option<RpcContextConfig>,
        }

        let (id, ReqParams { config }) = parse_params(request)?;
        let RpcContextConfig {
            commitment,
            min_context_slot,
        } = config.unwrap_or_default();
        let commitment = commitment.unwrap_or_default();

        let (id, _slot) = min_context_check(id, min_context_slot, commitment, &state)?;

        Ok(Self {
            state,
            id: id.into_owned(),
            commitment,
        })
    }

    async fn process(self) -> RpcRequestResult {
        let deadline = Instant::now() + self.state.request_timeout;

        // request
        let (tx, rx) = oneshot::channel();
        anyhow::ensure!(
            self.state
                .requests_tx
                .send(ReadRequest::LatestBlockhash {
                    deadline,
                    commitment: self.commitment,
                    tx
                })
                .await
                .is_ok(),
            "request channel is closed"
        );
        let Ok(result) = rx.await else {
            anyhow::bail!("rx channel is closed");
        };

        match result {
            ReadResultLatestBlockhash::Timeout => anyhow::bail!("timeout"),
            ReadResultLatestBlockhash::LatestBlockhash {
                slot,
                blockhash,
                last_valid_block_height,
            } => {
                let response = RpcResponse {
                    context: RpcResponseContext::new(slot),
                    value: RpcBlockhash {
                        blockhash,
                        last_valid_block_height,
                    },
                };
                Ok(jsonrpc_response_success(self.id, &response))
            }
            ReadResultLatestBlockhash::ReadError(error) => {
                anyhow::bail!("read error: {error}")
            }
        }
    }
}

#[derive(Debug)]
struct RpcRequestLeaderSchedule {
    state: Arc<State>,
    x_subscription_id: Arc<str>,
    id: Id<'static>,
    slot: Slot,
    is_processed: bool,
    identity: Option<String>,
}

impl RpcRequestHandler for RpcRequestLeaderSchedule {
    fn parse(
        state: Arc<State>,
        x_subscription_id: Arc<str>,
        _upstream_disabled: bool,
        request: Request<'_>,
    ) -> Result<Self, Vec<u8>> {
        #[derive(Debug, Default, Deserialize)]
        struct ReqParams {
            #[serde(default)]
            options: Option<RpcLeaderScheduleConfigWrapper>,
            #[serde(default)]
            config: Option<RpcLeaderScheduleConfig>,
        }

        let (id, ReqParams { options, config }) = if request.params.is_some() {
            parse_params(request)?
        } else {
            (request.id, Default::default())
        };
        let (slot, maybe_config) = options.map(|options| options.unzip()).unwrap_or_default();
        let config = maybe_config.or(config).unwrap_or_default();

        if let Some(identity) = &config.identity
            && let Err(error) = verify_pubkey(identity)
        {
            return Err(jsonrpc_response_error(id, error));
        }

        let (slot, is_processed) = match slot {
            Some(slot) => {
                if slot > state.stored_slots.processed_load() {
                    return Err(jsonrpc_response_success(id, json!(None::<()>)));
                }
                (slot, slot > state.stored_slots.confirmed_load())
            }
            None => match config.commitment.unwrap_or_default().commitment {
                CommitmentLevel::Processed => (state.stored_slots.processed_load(), true),
                CommitmentLevel::Confirmed => (state.stored_slots.confirmed_load(), false),
                CommitmentLevel::Finalized => (state.stored_slots.finalized_load(), false),
            },
        };

        Ok(Self {
            state,
            x_subscription_id,
            id: id.into_owned(),
            slot,
            is_processed,
            identity: config.identity,
        })
    }

    async fn process(self) -> RpcRequestResult {
        let deadline = Instant::now() + self.state.request_timeout;

        let Some(upstream) = self
            .state
            .get_upstream(ConfigRpcCallJson::GetLeaderSchedule)
        else {
            unreachable!();
        };

        let epoch = self.state.epoch_schedule.get_epoch(self.slot);
        upstream
            .get_leader_schedule(
                self.x_subscription_id,
                deadline,
                self.id,
                epoch,
                self.slot,
                self.is_processed,
                self.identity,
                REASON_ALWAYS,
            )
            .await
    }
}

#[derive(Debug)]
struct RpcRequestRecentPrioritizationFees {
    state: Arc<State>,
    id: Id<'static>,
    pubkeys: Vec<Pubkey>,
    percentile: Option<u16>,
}

impl RpcRequestHandler for RpcRequestRecentPrioritizationFees {
    fn parse(
        state: Arc<State>,
        _x_subscription_id: Arc<str>,
        _upstream_disabled: bool,
        request: Request<'_>,
    ) -> Result<Self, Vec<u8>> {
        #[derive(Debug, Default, Deserialize)]
        struct ReqParams {
            #[serde(default)]
            pubkey_strs: Option<Vec<String>>,
            #[serde(default)]
            config: Option<RpcRecentPrioritizationFeesConfig>,
        }

        let (
            id,
            ReqParams {
                pubkey_strs,
                config,
            },
        ) = if request.params.is_some() {
            parse_params(request)?
        } else {
            (request.id, Default::default())
        };

        let pubkey_strs = pubkey_strs.unwrap_or_default();
        if pubkey_strs.len() > MAX_TX_ACCOUNT_LOCKS {
            return Err(jsonrpc_response_error(
                id,
                jsonrpc_error_invalid_params::<()>(
                    format!("Too many inputs provided; max {MAX_TX_ACCOUNT_LOCKS}"),
                    None,
                ),
            ));
        }
        let pubkeys = match pubkey_strs
            .into_iter()
            .map(|pubkey_str| verify_pubkey(&pubkey_str))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(pubkeys) => pubkeys,
            Err(error) => {
                return Err(jsonrpc_response_error(id, error));
            }
        };

        let percentile = if state.grpf_percentile {
            let RpcRecentPrioritizationFeesConfig { percentile } = config.unwrap_or_default();
            if let Some(percentile) = percentile
                && percentile > 10_000
            {
                return Err(jsonrpc_response_error(
                    id,
                    jsonrpc_error_invalid_params::<()>(
                        "Percentile is too big; max value is 10000",
                        None,
                    ),
                ));
            }
            percentile
        } else {
            None
        };

        Ok(Self {
            state,
            id: id.into_owned(),
            pubkeys,
            percentile,
        })
    }

    async fn process(self) -> RpcRequestResult {
        let deadline = Instant::now() + self.state.request_timeout;

        // request
        let (tx, rx) = oneshot::channel();
        anyhow::ensure!(
            self.state
                .requests_tx
                .send(ReadRequest::RecentPrioritizationFees {
                    deadline,
                    pubkeys: self.pubkeys,
                    percentile: self.percentile,
                    tx,
                })
                .await
                .is_ok(),
            "request channel is closed"
        );
        let Ok(result) = rx.await else {
            anyhow::bail!("rx channel is closed");
        };

        match result {
            ReadResultRecentPrioritizationFees::Timeout => anyhow::bail!("timeout"),
            ReadResultRecentPrioritizationFees::Fees(fees) => {
                Ok(jsonrpc_response_success(self.id, &fees))
            }
        }
    }
}

#[derive(Debug)]
struct RpcRequestSignaturesForAddress {
    state: Arc<State>,
    x_subscription_id: Arc<str>,
    upstream_disabled: bool,
    id: Id<'static>,
    commitment: CommitmentConfig,
    address: Pubkey,
    before: Option<Signature>,
    until: Option<Signature>,
    limit: usize,
}

impl RpcRequestHandler for RpcRequestSignaturesForAddress {
    fn parse(
        state: Arc<State>,
        x_subscription_id: Arc<str>,
        upstream_disabled: bool,
        request: Request<'_>,
    ) -> Result<Self, Vec<u8>> {
        #[derive(Debug, Deserialize)]
        struct ReqParams {
            address: String,
            #[serde(default)]
            config: Option<RpcSignaturesForAddressConfig>,
        }

        let (id, ReqParams { address, config }) = parse_params(request)?;
        let RpcSignaturesForAddressConfig {
            before,
            until,
            limit,
            commitment,
            min_context_slot,
        } = config.unwrap_or_default();

        let (address, before, until, limit) = match verify_and_parse_signatures_for_address_params(
            address,
            before,
            until,
            limit,
            state.gsfa_limit,
        ) {
            Ok(value) => value,
            Err(error) => return Err(jsonrpc_response_error(id, error)),
        };

        let commitment = commitment.unwrap_or_default();
        if let Err(error) = check_is_at_least_confirmed(commitment) {
            return Err(jsonrpc_response_error(id, error));
        }

        let (id, _slot) = min_context_check(id, min_context_slot, commitment, &state)?;

        Ok(Self {
            state,
            x_subscription_id,
            upstream_disabled,
            id: id.into_owned(),
            commitment,
            address,
            before,
            until,
            limit,
        })
    }

    async fn process(self) -> RpcRequestResult {
        let deadline = Instant::now() + self.state.request_timeout;

        // request
        let (tx, rx) = oneshot::channel();
        anyhow::ensure!(
            self.state
                .requests_tx
                .send(ReadRequest::SignaturesForAddress {
                    deadline,
                    commitment: self.commitment,
                    address: self.address,
                    before: self.before,
                    until: self.until,
                    limit: self.limit,
                    tx,
                    x_subscription_id: Arc::clone(&self.x_subscription_id),
                })
                .await
                .is_ok(),
            "request channel is closed"
        );
        let Ok(result) = rx.await else {
            anyhow::bail!("rx channel is closed");
        };
        let (mut signatures, finished, mut before) = match result {
            ReadResultSignaturesForAddress::Timeout => anyhow::bail!("timeout"),
            ReadResultSignaturesForAddress::Signatures {
                signatures,
                finished,
                before,
            } => (signatures, finished, before),
            ReadResultSignaturesForAddress::ReadError(error) => {
                anyhow::bail!("read error: {error}")
            }
        };

        let limit = self.limit - signatures.len();
        let id = if !finished && !self.upstream_disabled && limit > 0 {
            if !signatures.is_empty() {
                before = signatures
                    .last()
                    .map(|sig| sig.signature.parse().expect("valid sig"));
            }

            match self.fetch_upstream(deadline, before, limit).await? {
                Ok((id, mut sigs)) => {
                    signatures.append(&mut sigs);
                    id
                }
                Err(error) => return Ok(error),
            }
        } else {
            self.id
        };

        Ok(jsonrpc_response_success(id, &signatures))
    }
}

impl RpcRequestSignaturesForAddress {
    async fn fetch_upstream(
        self,
        deadline: Instant,
        before: Option<Signature>,
        limit: usize,
    ) -> anyhow::Result<
        Result<(Id<'static>, Vec<RpcConfirmedTransactionStatusWithSignature>), Vec<u8>>,
    > {
        if let Some(upstream) = self
            .state
            .get_upstream(ConfigRpcCallJson::GetSignaturesForAddress)
        {
            let bytes = upstream
                .get_signatures_for_address(
                    self.x_subscription_id,
                    deadline,
                    &self.id,
                    self.address,
                    before,
                    self.until,
                    limit,
                    self.commitment,
                    REASON_INCOMPLETE_RESULTS,
                )
                .await?;

            let result: Response<Vec<RpcConfirmedTransactionStatusWithSignature>> =
                serde_json::from_slice(&bytes)
                    .map_err(|_error| anyhow::anyhow!("failed to parse json from upstream"))?;

            let value = match result.payload {
                ResponsePayload::Success(value) => value,
                ResponsePayload::Error(_) => return Ok(Err(bytes.to_vec())),
            };

            Ok(Ok((self.id, value.into_owned())))
        } else {
            Ok(Ok((self.id, vec![])))
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
enum GtfaTransactionDetails {
    #[default]
    Signatures,
    Full,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
enum GtfaSortOrder {
    #[default]
    Desc,
    Asc,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GtfaComparison {
    #[serde(default)]
    gte: Option<i64>,
    #[serde(default)]
    gt: Option<i64>,
    #[serde(default)]
    lte: Option<i64>,
    #[serde(default)]
    lt: Option<i64>,
    #[serde(default)]
    eq: Option<i64>,
}

impl GtfaComparison {
    fn matches(&self, value: i64) -> bool {
        self.gte.is_none_or(|bound| value >= bound)
            && self.gt.is_none_or(|bound| value > bound)
            && self.lte.is_none_or(|bound| value <= bound)
            && self.lt.is_none_or(|bound| value < bound)
            && self.eq.is_none_or(|bound| value == bound)
    }

    fn lower_bound(&self) -> Option<i64> {
        match (self.gte, self.gt, self.eq) {
            (_, _, Some(eq)) => Some(eq),
            (Some(gte), Some(gt), _) => Some(gte.max(gt.saturating_add(1))),
            (Some(gte), None, _) => Some(gte),
            (None, Some(gt), _) => Some(gt.saturating_add(1)),
            (None, None, _) => None,
        }
    }

    fn upper_bound(&self) -> Option<i64> {
        match (self.lte, self.lt, self.eq) {
            (_, _, Some(eq)) => Some(eq),
            (Some(lte), Some(lt), _) => Some(lte.min(lt.saturating_sub(1))),
            (Some(lte), None, _) => Some(lte),
            (None, Some(lt), _) => Some(lt.saturating_sub(1)),
            (None, None, _) => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
enum GtfaStatusFilter {
    #[default]
    Any,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
enum GtfaTokenAccountsFilter {
    #[default]
    None,
    BalanceChanged,
    All,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GtfaSignatureComparison {
    #[serde(default)]
    gte: Option<String>,
    #[serde(default)]
    gt: Option<String>,
    #[serde(default)]
    lte: Option<String>,
    #[serde(default)]
    lt: Option<String>,
}

#[derive(Debug, Clone, Copy, Default)]
struct GtfaSignatureFilter {
    gte: Option<Signature>,
    gt: Option<Signature>,
    lte: Option<Signature>,
    lt: Option<Signature>,
}

impl GtfaSignatureFilter {
    fn parse(raw: &GtfaSignatureComparison) -> Result<Self, ErrorObjectOwned> {
        Ok(Self {
            gte: raw.gte.as_deref().map(verify_signature).transpose()?,
            gt: raw.gt.as_deref().map(verify_signature).transpose()?,
            lte: raw.lte.as_deref().map(verify_signature).transpose()?,
            lt: raw.lt.as_deref().map(verify_signature).transpose()?,
        })
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct GtfaSignaturePositionFilter {
    gte: Option<(Slot, u32)>,
    gt: Option<(Slot, u32)>,
    lte: Option<(Slot, u32)>,
    lt: Option<(Slot, u32)>,
}

impl GtfaSignaturePositionFilter {
    fn matches(&self, position: (Slot, u32)) -> bool {
        self.gte.is_none_or(|bound| position >= bound)
            && self.gt.is_none_or(|bound| position > bound)
            && self.lte.is_none_or(|bound| position <= bound)
            && self.lt.is_none_or(|bound| position < bound)
    }

    fn lower_slot(&self) -> Option<Slot> {
        match (self.gte, self.gt) {
            (Some(a), Some(b)) => Some(a.0.max(b.0)),
            (Some(bound), None) | (None, Some(bound)) => Some(bound.0),
            (None, None) => None,
        }
    }

    fn upper_slot(&self) -> Option<Slot> {
        match (self.lte, self.lt) {
            (Some(a), Some(b)) => Some(a.0.min(b.0)),
            (Some(bound), None) | (None, Some(bound)) => Some(bound.0),
            (None, None) => None,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GtfaFilters {
    #[serde(default)]
    slot: Option<GtfaComparison>,
    #[serde(default)]
    block_time: Option<GtfaComparison>,
    #[serde(default)]
    signature: Option<GtfaSignatureComparison>,
    #[serde(default)]
    status: GtfaStatusFilter,
    #[serde(default)]
    token_accounts: GtfaTokenAccountsFilter,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GtfaOptions {
    #[serde(default)]
    transaction_details: GtfaTransactionDetails,
    #[serde(default)]
    sort_order: GtfaSortOrder,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    pagination_token: Option<String>,
    #[serde(default)]
    commitment: Option<CommitmentConfig>,
    #[serde(default)]
    min_context_slot: Option<Slot>,
    #[serde(default)]
    encoding: Option<UiTransactionEncoding>,
    #[serde(default)]
    max_supported_transaction_version: Option<u8>,
    #[serde(default)]
    filters: Option<GtfaFilters>,
}

#[derive(Debug, Clone, Copy)]
struct GtfaCursor {
    slot: Slot,
    transaction_index: usize,
}

impl GtfaCursor {
    fn encode(self) -> String {
        format!("{}:{}", self.slot, self.transaction_index)
    }

    fn decode(input: &str) -> Result<Self, ErrorObjectOwned> {
        let invalid = || jsonrpc_error_invalid_params::<()>("Invalid paginationToken", None);
        let (slot, txidx) = input.split_once(':').ok_or_else(invalid)?;
        Ok(Self {
            slot: slot.parse().map_err(|_error| invalid())?,
            transaction_index: txidx.parse().map_err(|_error| invalid())?,
        })
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct GtfaSignatureEntry {
    #[serde(flatten)]
    inner: RpcConfirmedTransactionStatusWithSignature,
    transaction_index: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct GtfaFullEntry {
    slot: Slot,
    transaction_index: Option<usize>,
    block_time: Option<UnixTimestamp>,
    #[serde(flatten)]
    transaction: EncodedTransactionWithStatusMeta,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GtfaResponse<T> {
    data: Vec<T>,
    pagination_token: Option<String>,
}

#[derive(Debug)]
struct RpcRequestTransactionsForAddress {
    state: Arc<State>,
    x_subscription_id: Arc<str>,
    id: Id<'static>,
    address: Pubkey,
    transaction_details: GtfaTransactionDetails,
    desc: bool,
    limit: usize,
    pagination_token: Option<GtfaCursor>,
    commitment: CommitmentConfig,
    encoding: UiTransactionEncoding,
    max_supported_transaction_version: Option<u8>,
    filters: Option<GtfaFilters>,
    token_accounts: GtfaTokenAccountsFilter,
    slot_filter_lower: Option<Slot>,
    slot_filter_upper: Option<Slot>,
    block_time_filter_lower: Option<UnixTimestamp>,
    block_time_filter_upper: Option<UnixTimestamp>,
    signature_filter: Option<GtfaSignatureFilter>,
}

impl RpcRequestHandler for RpcRequestTransactionsForAddress {
    fn parse(
        state: Arc<State>,
        x_subscription_id: Arc<str>,
        _upstream_disabled: bool,
        request: Request<'_>,
    ) -> Result<Self, Vec<u8>> {
        #[derive(Debug, Deserialize)]
        struct ReqParams {
            address: String,
            #[serde(default)]
            options: Option<GtfaOptions>,
        }

        let (id, ReqParams { address, options }) = parse_params(request)?;
        let GtfaOptions {
            transaction_details,
            sort_order,
            limit,
            pagination_token,
            commitment,
            min_context_slot,
            encoding,
            max_supported_transaction_version,
            filters,
        } = options.unwrap_or_default();

        let address = match verify_pubkey(&address) {
            Ok(address) => address,
            Err(error) => return Err(jsonrpc_response_error(id, error)),
        };

        let token_accounts = filters
            .as_ref()
            .map(|f| f.token_accounts)
            .unwrap_or_default();

        let default_limit = match transaction_details {
            GtfaTransactionDetails::Signatures => state.gtfa_limit_signatures,
            GtfaTransactionDetails::Full => state.gtfa_limit_full,
        };
        let limit = limit.unwrap_or(default_limit);
        if limit == 0 || limit > default_limit {
            return Err(jsonrpc_response_error(
                id,
                jsonrpc_error_invalid_params::<()>(
                    format!("Invalid limit; max {default_limit}"),
                    None,
                ),
            ));
        }

        let pagination_token = match pagination_token {
            Some(token) => match GtfaCursor::decode(&token) {
                Ok(cursor) => Some(cursor),
                Err(error) => return Err(jsonrpc_response_error(id, error)),
            },
            None => None,
        };

        let commitment = commitment.unwrap_or_default();
        if let Err(error) = check_is_at_least_confirmed(commitment) {
            return Err(jsonrpc_response_error(id, error));
        }

        let (id, _slot) = min_context_check(id, min_context_slot, commitment, &state)?;

        let encoding = encoding.unwrap_or(UiTransactionEncoding::Json);

        let (slot_filter_lower, slot_filter_upper) =
            match filters.as_ref().and_then(|f| f.slot.as_ref()) {
                Some(cmp) => (
                    cmp.lower_bound().map(|bound| bound.max(0) as Slot),
                    cmp.upper_bound().map(|bound| bound.max(0) as Slot),
                ),
                None => (None, None),
            };

        let (block_time_filter_lower, block_time_filter_upper) =
            match filters.as_ref().and_then(|f| f.block_time.as_ref()) {
                Some(cmp) => (cmp.lower_bound(), cmp.upper_bound()),
                None => (None, None),
            };

        let signature_filter = match filters.as_ref().and_then(|f| f.signature.as_ref()) {
            Some(cmp) => match GtfaSignatureFilter::parse(cmp) {
                Ok(filter) => Some(filter),
                Err(error) => return Err(jsonrpc_response_error(id, error)),
            },
            None => None,
        };

        Ok(Self {
            state,
            x_subscription_id,
            id: id.into_owned(),
            address,
            transaction_details,
            desc: matches!(sort_order, GtfaSortOrder::Desc),
            limit,
            pagination_token,
            commitment,
            encoding,
            max_supported_transaction_version,
            filters,
            token_accounts,
            slot_filter_lower,
            slot_filter_upper,
            block_time_filter_lower,
            block_time_filter_upper,
            signature_filter,
        })
    }

    async fn process(self) -> RpcRequestResult {
        let deadline = Instant::now() + self.state.request_timeout;

        // cursor = (slot, transaction_index) — directly maps to stored sfa_index values,
        let mut storage_cursor: Option<(Slot, u32)> = self
            .pagination_token
            .map(|c| (c.slot, c.transaction_index as u32));

        let mut page_cursor: Option<GtfaCursor> = None;
        let mut storage_finished = false;

        let mut sig_entries = Vec::new();
        let mut full_entries = Vec::new();
        let mut reached_limit = false;

        let signature_position_filter = match &self.signature_filter {
            Some(filter) => {
                let mut resolved = GtfaSignaturePositionFilter::default();

                if let Some(signature) = filter.gte {
                    match self.resolve_signature_position(signature, deadline).await? {
                        Some(position) => resolved.gte = Some(position),
                        None => return Ok(self.empty_response()),
                    }
                }
                if let Some(signature) = filter.gt {
                    match self.resolve_signature_position(signature, deadline).await? {
                        Some(position) => resolved.gt = Some(position),
                        None => return Ok(self.empty_response()),
                    }
                }
                if let Some(signature) = filter.lte {
                    match self.resolve_signature_position(signature, deadline).await? {
                        Some(position) => resolved.lte = Some(position),
                        None => return Ok(self.empty_response()),
                    }
                }
                if let Some(signature) = filter.lt {
                    match self.resolve_signature_position(signature, deadline).await? {
                        Some(position) => resolved.lt = Some(position),
                        None => return Ok(self.empty_response()),
                    }
                }

                Some(resolved)
            }
            None => None,
        };

        // refine the slot range with bounds derived from the resolved signature positions
        let slot_filter_lower = match (
            self.slot_filter_lower,
            signature_position_filter.and_then(|f| f.lower_slot()),
        ) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (Some(bound), None) | (None, Some(bound)) => Some(bound),
            (None, None) => None,
        };
        let slot_filter_upper = match (
            self.slot_filter_upper,
            signature_position_filter.and_then(|f| f.upper_slot()),
        ) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(bound), None) | (None, Some(bound)) => Some(bound),
            (None, None) => None,
        };

        loop {
            let collected = sig_entries.len() + full_entries.len();
            let remaining = self.limit - collected;
            if remaining == 0 {
                break;
            }

            let request_limit = if self.filters.is_some() {
                remaining
                    .saturating_mul(4)
                    .min(self.state.gtfa_limit_signatures)
                    .max(remaining)
            } else {
                remaining
            };

            let (tx, rx) = oneshot::channel();
            anyhow::ensure!(
                self.state
                    .requests_tx
                    .send(ReadRequest::TransactionsForAddress {
                        deadline,
                        commitment: self.commitment,
                        address: self.address,
                        desc: self.desc,
                        cursor: storage_cursor,
                        slot_filter_lower,
                        slot_filter_upper,
                        block_time_filter_lower: self.block_time_filter_lower,
                        block_time_filter_upper: self.block_time_filter_upper,
                        limit: request_limit,
                        tx,
                        x_subscription_id: Arc::clone(&self.x_subscription_id),
                    })
                    .await
                    .is_ok(),
                "request channel is closed"
            );
            let Ok(result) = rx.await else {
                anyhow::bail!("rx channel is closed");
            };

            let (batch, batch_txindices, batch_token_flags, finished) = match result {
                ReadResultTransactionsForAddress::Timeout => anyhow::bail!("timeout"),
                ReadResultTransactionsForAddress::Transactions {
                    signatures,
                    transaction_indices,
                    token_owner_flags,
                    finished,
                } => (signatures, transaction_indices, token_owner_flags, finished),
                ReadResultTransactionsForAddress::ReadError(error) => {
                    anyhow::bail!("read error: {error}")
                }
            };
            storage_finished = finished;
            let batch_was_empty = batch.is_empty();
            reached_limit = false;

            let mut pending_full = Vec::new();

            for ((raw, txidx), flags) in batch
                .iter()
                .zip(batch_txindices.iter().copied())
                .zip(batch_token_flags.iter().copied())
            {
                // Always advance storage cursor so next batch resumes after this raw item.
                storage_cursor = Some((raw.slot, txidx));

                let is_token_owner = flags & 0x01 != 0;
                let balance_changed = flags & 0x02 != 0;
                let token_ok = match self.token_accounts {
                    GtfaTokenAccountsFilter::None => !is_token_owner,
                    GtfaTokenAccountsFilter::All => true,
                    GtfaTokenAccountsFilter::BalanceChanged => !is_token_owner || balance_changed,
                };
                if !token_ok {
                    continue;
                }

                if let Some(filters) = &self.filters {
                    if let Some(block_time_filter) = &filters.block_time {
                        match raw.block_time {
                            Some(block_time) if block_time_filter.matches(block_time) => {}
                            _ => continue,
                        }
                    }

                    let status_ok = match filters.status {
                        GtfaStatusFilter::Any => true,
                        GtfaStatusFilter::Succeeded => raw.err.is_none(),
                        GtfaStatusFilter::Failed => raw.err.is_some(),
                    };
                    if !status_ok {
                        continue;
                    }
                }

                if let Some(position_filter) = &signature_position_filter
                    && !position_filter.matches((raw.slot, txidx))
                {
                    continue;
                }

                let transaction_index = Some(txidx as usize);

                match self.transaction_details {
                    GtfaTransactionDetails::Signatures => {
                        sig_entries.push(GtfaSignatureEntry {
                            inner: raw.clone(),
                            transaction_index,
                        });
                    }
                    GtfaTransactionDetails::Full => {
                        pending_full.push((raw.clone(), transaction_index));
                    }
                }

                page_cursor = Some(GtfaCursor {
                    slot: raw.slot,
                    transaction_index: txidx as usize,
                });

                if sig_entries.len() + full_entries.len() + pending_full.len() == self.limit {
                    reached_limit = true;
                    break;
                }
            }

            if !pending_full.is_empty() {
                // Fetch + decode + encode all pending full entries concurrently instead of
                // one-by-one: overlaps storage round-trips and offloads the CPU-heavy
                // encode step (see build_full_entry) off the async runtime workers.
                let results =
                    try_join_all(pending_full.into_iter().map(|(raw, transaction_index)| {
                        let signature: Signature = raw.signature.parse().expect("valid signature");
                        self.build_full_entry(
                            signature,
                            raw.slot,
                            raw.block_time,
                            transaction_index,
                            deadline,
                        )
                    }))
                    .await?;
                for result in results {
                    match result {
                        Ok(entry) => full_entries.push(entry),
                        Err(response) => return Ok(response),
                    }
                }
            }

            if reached_limit || storage_finished || batch_was_empty {
                break;
            }
        }

        let pagination_token = if storage_finished && !reached_limit {
            None
        } else {
            page_cursor.map(GtfaCursor::encode)
        };

        Ok(match self.transaction_details {
            GtfaTransactionDetails::Signatures => jsonrpc_response_success(
                self.id,
                &GtfaResponse {
                    data: sig_entries,
                    pagination_token,
                },
            ),
            GtfaTransactionDetails::Full => jsonrpc_response_success(
                self.id,
                &GtfaResponse {
                    data: full_entries,
                    pagination_token,
                },
            ),
        })
    }
}

impl RpcRequestTransactionsForAddress {
    async fn resolve_signature_position(
        &self,
        signature: Signature,
        deadline: Instant,
    ) -> anyhow::Result<Option<(Slot, u32)>> {
        let (tx, rx) = oneshot::channel();
        anyhow::ensure!(
            self.state
                .requests_tx
                .send(ReadRequest::SignaturePosition {
                    deadline,
                    address: self.address,
                    signature,
                    tx,
                    x_subscription_id: Arc::clone(&self.x_subscription_id),
                })
                .await
                .is_ok(),
            "request channel is closed"
        );
        let Ok(result) = rx.await else {
            anyhow::bail!("rx channel is closed");
        };
        match result {
            ReadResultSignaturePosition::Position(position) => Ok(position),
            ReadResultSignaturePosition::Timeout => anyhow::bail!("timeout"),
            ReadResultSignaturePosition::ReadError(error) => anyhow::bail!("read error: {error}"),
        }
    }

    fn empty_response(&self) -> Vec<u8> {
        jsonrpc_response_success(
            self.id.clone(),
            &GtfaResponse::<GtfaSignatureEntry> {
                data: vec![],
                pagination_token: None,
            },
        )
    }

    async fn fetch_transaction(
        &self,
        signature: Signature,
        deadline: Instant,
    ) -> anyhow::Result<Option<(Slot, Option<UnixTimestamp>, Vec<u8>)>> {
        let (tx, rx) = oneshot::channel();
        anyhow::ensure!(
            self.state
                .requests_tx
                .send(ReadRequest::Transaction {
                    deadline,
                    signature,
                    tx,
                    x_subscription_id: Arc::clone(&self.x_subscription_id),
                })
                .await
                .is_ok(),
            "request channel is closed"
        );
        let Ok(result) = rx.await else {
            anyhow::bail!("rx channel is closed");
        };

        Ok(match result {
            ReadResultTransaction::Timeout => anyhow::bail!("timeout"),
            ReadResultTransaction::NotFound => None,
            // `index` isn't used here: `build_full_entry` (the only caller) already has
            // an authoritative transaction_index from the address-signature index scan.
            ReadResultTransaction::Transaction {
                slot,
                block_time,
                bytes,
                index: _,
            } => Some((slot, block_time, bytes)),
            ReadResultTransaction::ReadError(error) => anyhow::bail!("read error: {error}"),
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn build_full_entry(
        &self,
        signature: Signature,
        index_slot: Slot,
        index_block_time: Option<UnixTimestamp>,
        transaction_index: Option<usize>,
        deadline: Instant,
    ) -> anyhow::Result<Result<GtfaFullEntry, Vec<u8>>> {
        let Some((slot, block_time, bytes)) = self.fetch_transaction(signature, deadline).await?
        else {
            anyhow::bail!(
                "transaction {signature} referenced by address index but not found in storage"
            );
        };
        let block_time = block_time.or(index_block_time);

        let encoding = self.encoding;
        let max_supported_transaction_version = self.max_supported_transaction_version;
        let id = self.id.clone();

        match spawn_blocking(move || {
            let tx_with_meta = match generated::ConfirmedTransaction::decode(bytes.as_ref()) {
                Ok(tx) => match TransactionWithStatusMeta::try_from(tx) {
                    Ok(tx_with_meta) => tx_with_meta,
                    Err(error) => {
                        error!(slot, ?error, "failed to decode transaction");
                        anyhow::bail!("failed to decode transaction")
                    }
                },
                Err(error) => {
                    error!(
                        slot,
                        ?error,
                        "failed to decode transaction protobuf / bincode"
                    );
                    anyhow::bail!("failed to decode transaction protobuf / bincode")
                }
            };

            let confirmed_tx = ConfirmedTransactionWithStatusMeta {
                slot,
                tx_with_meta,
                block_time,
                index: transaction_index.map_or(0, |index| index as u32),
            };
            match confirmed_tx.encode(encoding, max_supported_transaction_version) {
                Ok(encoded) => Ok(Ok(GtfaFullEntry {
                    slot: index_slot,
                    transaction_index,
                    block_time,
                    transaction: encoded.transaction,
                })),
                Err(error) => Ok(Err(jsonrpc_response_error_custom(
                    id,
                    RpcCustomError::from(error),
                ))),
            }
        })
        .await
        {
            Ok(result) => result,
            Err(error) => anyhow::bail!("encode task panicked: {error}"),
        }
    }
}

#[derive(Debug)]
struct RpcRequestSignatureStatuses {
    state: Arc<State>,
    x_subscription_id: Arc<str>,
    upstream_disabled: bool,
    id: Id<'static>,
    signatures: Vec<Signature>,
    search_transaction_history: bool,
}

impl RpcRequestHandler for RpcRequestSignatureStatuses {
    fn parse(
        state: Arc<State>,
        x_subscription_id: Arc<str>,
        upstream_disabled: bool,
        request: Request<'_>,
    ) -> Result<Self, Vec<u8>> {
        #[derive(Debug, Deserialize)]
        struct ReqParams {
            signature_strs: Vec<String>,
            #[serde(default)]
            config: Option<RpcSignatureStatusConfig>,
        }

        let (
            id,
            ReqParams {
                signature_strs,
                config,
            },
        ) = parse_params(request)?;

        if signature_strs.len() > MAX_GET_SIGNATURE_STATUSES_QUERY_ITEMS {
            let message =
                format!("Too many inputs provided; max {MAX_GET_SIGNATURE_STATUSES_QUERY_ITEMS}");
            return Err(jsonrpc_response_error(
                id,
                jsonrpc_error_invalid_params::<()>(message, None),
            ));
        }

        let mut signatures: Vec<Signature> = Vec::with_capacity(signature_strs.len());
        for signature_str in signature_strs {
            match verify_signature(&signature_str) {
                Ok(signature) => {
                    signatures.push(signature);
                }
                Err(error) => return Err(jsonrpc_response_error(id, error)),
            }
        }

        let search_transaction_history = config
            .map(|x| x.search_transaction_history)
            .unwrap_or(false);

        if search_transaction_history && !state.gss_transaction_history {
            return Err(jsonrpc_response_error_custom(
                id,
                RpcCustomError::TransactionHistoryNotAvailable,
            ));
        }

        Ok(Self {
            state,
            x_subscription_id,
            upstream_disabled,
            id: id.into_owned(),
            signatures,
            search_transaction_history,
        })
    }

    async fn process(self) -> RpcRequestResult {
        let deadline = Instant::now() + self.state.request_timeout;

        // request
        let (tx, rx) = oneshot::channel();
        anyhow::ensure!(
            self.state
                .requests_tx
                .send(ReadRequest::SignatureStatuses {
                    deadline,
                    signatures: self.signatures.clone(),
                    search_transaction_history: self.search_transaction_history,
                    tx,
                    x_subscription_id: Arc::clone(&self.x_subscription_id),
                })
                .await
                .is_ok(),
            "request channel is closed"
        );
        let Ok(result) = rx.await else {
            anyhow::bail!("rx channel is closed");
        };
        let mut statuses = match result {
            ReadResultSignatureStatuses::Timeout => anyhow::bail!("timeout"),
            ReadResultSignatureStatuses::Signatures(signatures) => signatures,
            ReadResultSignatureStatuses::ReadError(error) => {
                anyhow::bail!("read error: {error}")
            }
        };

        if self.search_transaction_history
            && !self.upstream_disabled
            && self
                .state
                .get_upstream(ConfigRpcCallJson::GetSignatureStatuses)
                .is_some()
            && statuses.iter().any(|status| status.is_none())
        {
            let mut signatures_history = Vec::new();
            for (signature, status) in self.signatures.iter().zip(statuses.iter()) {
                if status.is_none() {
                    signatures_history.push(signature);
                }
            }

            let mut signatures_upstream =
                match self.fetch_upstream(deadline, signatures_history).await? {
                    Ok(sigs) => sigs,
                    Err(error) => return Ok(error),
                };

            let mut index = 0;
            for status in statuses.iter_mut() {
                if status.is_none() {
                    *status = signatures_upstream[index].take();
                    index += 1;
                }
            }
        }

        let response = RpcResponse {
            context: RpcResponseContext::new(self.state.stored_slots.processed_load()),
            value: statuses,
        };
        Ok(jsonrpc_response_success(self.id, &response))
    }
}

impl RpcRequestSignatureStatuses {
    async fn fetch_upstream(
        &self,
        deadline: Instant,
        signatures: Vec<&Signature>,
    ) -> anyhow::Result<Result<Vec<Option<TransactionStatus>>, Vec<u8>>> {
        if let Some(upstream) = self
            .state
            .get_upstream(ConfigRpcCallJson::GetSignatureStatuses)
        {
            let bytes = upstream
                .get_signature_statuses(
                    Arc::clone(&self.x_subscription_id),
                    deadline,
                    &self.id,
                    signatures,
                    REASON_MISSING_STATUS,
                )
                .await?;

            let result: Response<RpcResponse<Vec<Option<TransactionStatus>>>> =
                serde_json::from_slice(&bytes)
                    .map_err(|_error| anyhow::anyhow!("failed to parse json from upstream"))?;

            if let ResponsePayload::Error(_) = &result.payload {
                return Ok(Err(bytes.to_vec()));
            }

            let ResponsePayload::Success(value) = result.payload else {
                unreachable!();
            };
            Ok(Ok(value.into_owned().value))
        } else {
            Ok(Ok(vec![]))
        }
    }
}

#[derive(Debug)]
struct RpcRequestSlot;

impl RpcRequestHandler for RpcRequestSlot {
    fn parse(
        state: Arc<State>,
        _x_subscription_id: Arc<str>,
        _upstream_disabled: bool,
        request: Request<'_>,
    ) -> Result<Self, Vec<u8>> {
        #[derive(Debug, Default, Deserialize)]
        struct ReqParams {
            #[serde(default)]
            config: Option<RpcContextConfig>,
        }

        let (id, ReqParams { config }) = if request.params.is_some() {
            parse_params(request)?
        } else {
            (request.id, Default::default())
        };
        let RpcContextConfig {
            commitment,
            min_context_slot,
        } = config.unwrap_or_default();
        let commitment = commitment.unwrap_or_default();

        let context_slot = match commitment.commitment {
            CommitmentLevel::Processed => state.stored_slots.processed_load(),
            CommitmentLevel::Confirmed => state.stored_slots.confirmed_load(),
            CommitmentLevel::Finalized => state.stored_slots.finalized_load(),
        };

        if let Some(min_context_slot) = min_context_slot
            && context_slot < min_context_slot
        {
            return Err(jsonrpc_response_error_custom(
                id,
                RpcCustomError::MinContextSlotNotReached { context_slot },
            ));
        }

        Err(jsonrpc_response_success(id, context_slot))
    }
}

#[derive(Debug)]
struct RpcRequestTransaction {
    state: Arc<State>,
    x_subscription_id: Arc<str>,
    upstream_disabled: bool,
    id: Id<'static>,
    signature: Signature,
    commitment: CommitmentConfig,
    encoding: UiTransactionEncoding,
    max_supported_transaction_version: Option<u8>,
}

impl RpcRequestHandler for RpcRequestTransaction {
    fn parse(
        state: Arc<State>,
        x_subscription_id: Arc<str>,
        upstream_disabled: bool,
        request: Request<'_>,
    ) -> Result<Self, Vec<u8>> {
        #[derive(Debug, Deserialize)]
        struct ReqParams {
            signature_str: String,
            #[serde(default)]
            config: Option<RpcEncodingConfigWrapper<RpcTransactionConfig>>,
        }

        let (
            id,
            ReqParams {
                signature_str,
                config,
            },
        ) = parse_params(request)?;

        let signature = match verify_signature(&signature_str) {
            Ok(signature) => signature,
            Err(error) => return Err(jsonrpc_response_error(id, error)),
        };

        let config = config
            .map(|config| config.convert_to_current())
            .unwrap_or_default();
        let encoding = config.encoding.unwrap_or(UiTransactionEncoding::Json);
        let max_supported_transaction_version = config.max_supported_transaction_version;
        let commitment = config.commitment.unwrap_or_default();
        if let Err(error) = check_is_at_least_confirmed(commitment) {
            return Err(jsonrpc_response_error(id, error));
        }

        Ok(Self {
            state,
            x_subscription_id,
            upstream_disabled,
            id: id.into_owned(),
            signature,
            commitment,
            encoding,
            max_supported_transaction_version,
        })
    }

    async fn process(self) -> RpcRequestResult {
        let deadline = Instant::now() + self.state.request_timeout;

        // request
        let (tx, rx) = oneshot::channel();
        anyhow::ensure!(
            self.state
                .requests_tx
                .send(ReadRequest::Transaction {
                    deadline,
                    signature: self.signature,
                    tx,
                    x_subscription_id: Arc::clone(&self.x_subscription_id),
                })
                .await
                .is_ok(),
            "request channel is closed"
        );
        let Ok(result) = rx.await else {
            anyhow::bail!("rx channel is closed");
        };
        let (slot, block_time, bytes, index) = match result {
            ReadResultTransaction::Timeout => anyhow::bail!("timeout"),
            ReadResultTransaction::NotFound => {
                return self
                    .fetch_upstream(deadline, REASON_NOT_FOUND_LOCALLY)
                    .await;
            }
            ReadResultTransaction::Transaction {
                slot,
                block_time,
                bytes,
                index,
            } => (slot, block_time, bytes, index),
            ReadResultTransaction::ReadError(error) => anyhow::bail!("read error: {error}"),
        };

        // verify commitment
        if self.commitment.is_finalized() && self.state.stored_slots.finalized_load() < slot {
            return Ok(jsonrpc_response_success(self.id, json!(None::<()>)));
        }

        // verify that we still have data for that block (i.e. we read correct data)
        if slot <= self.state.stored_slots.first_available_load() {
            return self.fetch_upstream(deadline, REASON_BELOW_RETENTION).await;
        }

        // parse, encode and serialize
        process_with_workers(RpcRequestTransactionWorkRequest::create(
            self, slot, block_time, bytes, index,
        ))
        .await
    }
}

impl RpcRequestTransaction {
    async fn fetch_upstream(self, deadline: Instant, reason: &'static str) -> RpcRequestResult {
        if self.upstream_disabled {
            return Ok(jsonrpc_response_success(self.id, None::<()>));
        }

        if let Some(upstream) = self.state.get_upstream(ConfigRpcCallJson::GetTransaction) {
            debug!(%self.signature, reason = ?reason, "getTransaction signature going upstream");
            upstream
                .get_transaction(
                    self.x_subscription_id,
                    deadline,
                    &self.id,
                    self.signature,
                    self.commitment,
                    self.encoding,
                    self.max_supported_transaction_version,
                    reason,
                )
                .await
        } else {
            Ok(jsonrpc_response_error_custom(
                self.id,
                RpcCustomError::TransactionHistoryNotAvailable,
            ))
        }
    }
}

#[derive(Debug)]
pub struct RpcRequestTransactionWorkRequest {
    x_subscription_id: Arc<str>,
    id: Id<'static>,
    encoding: UiTransactionEncoding,
    max_supported_transaction_version: Option<u8>,
    slot: Slot,
    block_time: Option<UnixTimestamp>,
    bytes: Vec<u8>,
    index: u32,
    tx: Option<oneshot::Sender<RpcRequestResult>>,
}

impl RpcRequestTransactionWorkRequest {
    fn create(
        request: RpcRequestTransaction,
        slot: Slot,
        block_time: Option<UnixTimestamp>,
        bytes: Vec<u8>,
        index: u32,
    ) -> (Arc<State>, WorkRequest, oneshot::Receiver<RpcRequestResult>) {
        let (tx, rx) = oneshot::channel();
        let this = Self {
            x_subscription_id: request.x_subscription_id,
            id: request.id,
            encoding: request.encoding,
            max_supported_transaction_version: request.max_supported_transaction_version,
            slot,
            block_time,
            bytes,
            index,
            tx: Some(tx),
        };
        (request.state, WorkRequest::Transaction(this), rx)
    }

    pub fn process(mut self) {
        if let Some(tx) = self.tx.take() {
            let ts = quanta::Instant::now();
            let _ = tx.send(Self::process2(
                self.bytes,
                self.slot,
                self.block_time,
                self.index,
                self.id,
                self.encoding,
                self.max_supported_transaction_version,
            ));
            gauge!(
                RPC_WORKERS_CPU_SECONDS_TOTAL,
                "x_subscription_id" => self.x_subscription_id,
                "method" => "getTransaction"
            )
            .increment(duration_to_seconds(ts.elapsed()));
        }
    }

    fn process2(
        bytes: Vec<u8>,
        slot: Slot,
        block_time: Option<UnixTimestamp>,
        index: u32,
        id: Id<'static>,
        encoding: UiTransactionEncoding,
        max_supported_transaction_version: Option<u8>,
    ) -> RpcRequestResult {
        // parse
        let tx_with_meta = match generated::ConfirmedTransaction::decode(bytes.as_ref()) {
            Ok(tx) => match TransactionWithStatusMeta::try_from(tx) {
                Ok(tx_with_meta) => tx_with_meta,
                Err(error) => {
                    error!(slot, ?error, "failed to decode transaction");
                    anyhow::bail!("failed to decode transaction")
                }
            },
            Err(error) => {
                error!(
                    slot,
                    ?error,
                    "failed to decode transaction protobuf / bincode"
                );
                anyhow::bail!("failed to decode transaction protobuf / bincode")
            }
        };

        // encode
        let confirmed_tx = ConfirmedTransactionWithStatusMeta {
            slot,
            tx_with_meta,
            block_time,
            index,
        };
        let tx = match confirmed_tx.encode(encoding, max_supported_transaction_version) {
            Ok(tx) => tx,
            Err(error) => {
                return Ok(jsonrpc_response_error_custom(
                    id,
                    RpcCustomError::from(error),
                ));
            }
        };

        // serialize
        Ok(jsonrpc_response_success(id, &tx))
    }
}

#[derive(Debug)]
struct RpcRequestHealth;

impl RpcRequestHandler for RpcRequestHealth {
    fn parse(
        state: Arc<State>,
        _x_subscription_id: Arc<str>,
        _upstream_disabled: bool,
        request: Request<'_>,
    ) -> Result<Self, Vec<u8>> {
        let request = no_params_expected(request)?;
        if state.health_disabled.load(Ordering::Relaxed) {
            return Err(jsonrpc_response_error_custom(
                request.id,
                RpcCustomError::NodeUnhealthy {
                    num_slots_behind: None,
                },
            ));
        }
        let cluster_slot = state.cluster_processed_slot.load(Ordering::Relaxed);
        if cluster_slot == 0 {
            return Err(jsonrpc_response_success(request.id, "ok"));
        }
        let local_slot = state.stored_slots.processed_load();
        if local_slot >= cluster_slot.saturating_sub(state.health_check_slot_distance) {
            Err(jsonrpc_response_success(request.id, "ok"))
        } else {
            let num_slots_behind = cluster_slot.saturating_sub(local_slot);
            Err(jsonrpc_response_error_custom(
                request.id,
                RpcCustomError::NodeUnhealthy {
                    num_slots_behind: Some(num_slots_behind),
                },
            ))
        }
    }
}

struct RpcRequestVersion;

impl RpcRequestHandler for RpcRequestVersion {
    fn parse(
        _state: Arc<State>,
        _x_subscription_id: Arc<str>,
        _upstream_disabled: bool,
        request: Request<'_>,
    ) -> Result<Self, Vec<u8>> {
        let request = no_params_expected(request)?;
        let version = solana_version::Version::default();
        Err(jsonrpc_response_success(
            request.id,
            json!(RpcVersionInfo {
                solana_core: version.to_string(),
                feature_set: Some(version.feature_set),
            }),
        ))
    }
}

#[derive(Debug)]
pub struct RpcRequestIsBlockhashValid {
    state: Arc<State>,
    id: Id<'static>,
    blockhash: String,
    commitment: CommitmentConfig,
}

impl RpcRequestHandler for RpcRequestIsBlockhashValid {
    fn parse(
        state: Arc<State>,
        _x_subscription_id: Arc<str>,
        _upstream_disabled: bool,
        request: Request<'_>,
    ) -> Result<Self, Vec<u8>> {
        #[derive(Debug, Deserialize)]
        struct ReqParams {
            blockhash: String,
            #[serde(default)]
            config: Option<RpcContextConfig>,
        }

        let (id, ReqParams { blockhash, config }) = parse_params(request)?;
        let RpcContextConfig {
            commitment,
            min_context_slot,
        } = config.unwrap_or_default();
        let commitment = commitment.unwrap_or_default();

        let (id, _slot) = min_context_check(id, min_context_slot, commitment, &state)?;

        if let Err(error) = Hash::from_str(&blockhash) {
            return Err(jsonrpc_response_error(
                id,
                jsonrpc_error_invalid_params::<()>(format!("{error:?}"), None),
            ));
        }

        Ok(Self {
            state,
            id: id.into_owned(),
            blockhash,
            commitment,
        })
    }

    async fn process(self) -> RpcRequestResult {
        let deadline = Instant::now() + self.state.request_timeout;

        // request
        let (tx, rx) = oneshot::channel();
        anyhow::ensure!(
            self.state
                .requests_tx
                .send(ReadRequest::BlockhashValid {
                    deadline,
                    blockhash: self.blockhash,
                    commitment: self.commitment,
                    tx
                })
                .await
                .is_ok(),
            "request channel is closed"
        );
        let Ok(result) = rx.await else {
            anyhow::bail!("rx channel is closed");
        };

        match result {
            ReadResultBlockhashValid::Timeout => anyhow::bail!("timeout"),
            ReadResultBlockhashValid::Blockhash { slot, is_valid } => {
                let response = RpcResponse {
                    context: RpcResponseContext::new(slot),
                    value: is_valid,
                };
                Ok(jsonrpc_response_success(self.id, &response))
            }
            ReadResultBlockhashValid::ReadError(error) => {
                anyhow::bail!("read error: {error}")
            }
        }
    }
}
