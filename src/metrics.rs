use {
    crate::{
        config::ConfigMetrics, storage::slots::StoredSlots, util::tokens_match,
        version::VERSION as VERSION_INFO,
    },
    anyhow::Context,
    http_body_util::Full as BodyFull,
    hyper::{
        Request, Response, StatusCode,
        body::{Bytes, Incoming as BodyIncoming},
        header::{AUTHORIZATION, WWW_AUTHENTICATE},
        service::service_fn,
    },
    hyper_util::{
        rt::tokio::{TokioExecutor, TokioIo},
        server::conn::auto::Builder as ServerBuilder,
    },
    metrics::{counter, describe_counter, describe_gauge, describe_histogram},
    metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle},
    richat_shared::jsonrpc::metrics::{
        RPC_REQUESTS_DURATION_SECONDS, describe as describe_jsonrpc_metrics,
    },
    std::{future::Future, sync::Arc, time::Duration},
    tokio::{net::TcpListener, task::JoinError, time::sleep},
    tracing::{error, info},
};

pub const SOURCE_GRPC_VERSION: &str = "source_grpc_version"; // hostname
pub const SOURCE_HTTP_BLOCK: &str = "source_http_block";

pub const STORAGE_STORED_SLOTS: &str = "storage_stored_slots"; // type
pub const STORAGE_FILES_SPACE: &str = "storage_files_space_bytes"; // id, type

pub const READ_DISK_SECONDS_TOTAL: &str = "read_disk_seconds_total"; // x_subscription_id, type

pub const WRITE_BLOCK_SYNC_SECONDS: &str = "write_block_sync_seconds";

pub const RPC_WORKERS_CPU_SECONDS_TOTAL: &str = "rpc_workers_cpu_seconds_total"; // x_subscription_id, method

pub const RPC_UPSTREAM_REQUESTS_TOTAL: &str = "rpc_upstream_requests_total"; // x_subscription_id, upstream, method, reason
pub const RPC_UPSTREAM_DURATION_SECONDS: &str = "rpc_upstream_duration_seconds"; // x_subscription_id, upstream, method, reason
pub const RPC_UPSTREAM_BANDWIDTH_TOTAL: &str = "rpc_upstream_bandwidth_total"; // x_subscription_id, upstream, method

pub fn setup() -> anyhow::Result<PrometheusHandle> {
    let handle = PrometheusBuilder::new()
        .set_buckets_for_metric(
            Matcher::Full(SOURCE_HTTP_BLOCK.to_owned()),
            &[
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ],
        )?
        .set_buckets_for_metric(
            Matcher::Full(WRITE_BLOCK_SYNC_SECONDS.to_owned()),
            &[0.005, 0.01, 0.02, 0.03, 0.04, 0.05, 0.075, 0.1, 0.2, 0.4],
        )?
        .set_buckets_for_metric(
            Matcher::Full(RPC_REQUESTS_DURATION_SECONDS.to_owned()),
            &[
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ],
        )?
        .set_buckets_for_metric(
            Matcher::Full(RPC_UPSTREAM_DURATION_SECONDS.to_owned()),
            &[
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ],
        )?
        .install_recorder()
        .context("failed to install prometheus exporter")?;

    describe_jsonrpc_metrics();

    describe_counter!("version", "Alpamayo version info");
    counter!(
        "version",
        "buildts" => VERSION_INFO.buildts,
        "git" => VERSION_INFO.git,
        "package" => VERSION_INFO.package,
        "proto_dragonsmouth" => VERSION_INFO.proto,
        "proto_richat" => VERSION_INFO.proto_richat,
        "rustc" => VERSION_INFO.rustc,
        "solana" => VERSION_INFO.solana,
        "version" => VERSION_INFO.version,
    )
    .absolute(1);

    describe_gauge!(SOURCE_GRPC_VERSION, "Current Geyser gRPC as source");
    describe_histogram!(
        SOURCE_HTTP_BLOCK,
        "RPC requests to fetch full block to fill db"
    );

    describe_gauge!(STORAGE_STORED_SLOTS, "Stored slots in db");
    describe_gauge!(STORAGE_FILES_SPACE, "Storage space in files for blocks");

    describe_gauge!(
        READ_DISK_SECONDS_TOTAL,
        "Read disk time by x-subscription-id and type"
    );

    describe_histogram!(WRITE_BLOCK_SYNC_SECONDS, "Write block sync time");

    describe_gauge!(
        RPC_WORKERS_CPU_SECONDS_TOTAL,
        "CPU consumption by RPC workers by x-subscription-id and method"
    );

    describe_counter!(
        RPC_UPSTREAM_REQUESTS_TOTAL,
        "Number of RPC requests to upstream by x-subscription-id, upstream name, method, timeout and reason"
    );
    describe_histogram!(
        RPC_UPSTREAM_DURATION_SECONDS,
        "RPC request time to upstream by x-subscription-id, upstream name, method and reason"
    );
    describe_counter!(
        RPC_UPSTREAM_BANDWIDTH_TOTAL,
        "Number of bytes received by RPC requests to upstream by x-subscription-id, upstream and method"
    );

    Ok(handle)
}

pub async fn spawn_server(
    ConfigMetrics { endpoint, token }: ConfigMetrics,
    handle: PrometheusHandle,
    stored_slots: StoredSlots,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<impl Future<Output = Result<(), JoinError>>> {
    let recorder_handle = handle.clone();
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(1)).await;
            recorder_handle.run_upkeep();
        }
    });

    let listener = TcpListener::bind(endpoint)
        .await
        .with_context(|| format!("failed to bind metrics server at {endpoint}"))?;
    info!("start metrics server at: {endpoint}");

    let token: Option<Arc<str>> = token.map(Into::into);
    Ok(tokio::spawn(async move {
        tokio::pin!(shutdown);
        loop {
            let stream = tokio::select! {
                maybe_conn = listener.accept() => match maybe_conn {
                    Ok((stream, _addr)) => stream,
                    Err(error) => {
                        error!("failed to accept new connection: {error}");
                        break;
                    }
                },
                () = &mut shutdown => {
                    info!("metrics server shutdown");
                    break;
                }
            };

            let handle = handle.clone();
            let stored_slots = stored_slots.clone();
            let token = token.clone();
            tokio::spawn(async move {
                let service = service_fn(move |req: Request<BodyIncoming>| {
                    let (status, body) = match req.uri().path() {
                        "/health" => (StatusCode::OK, Bytes::from("OK")),
                        "/ready" if stored_slots.is_ready() => (StatusCode::OK, Bytes::from("OK")),
                        "/ready" => (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Bytes::from("Service is not ready"),
                        ),
                        "/metrics" if is_authorized(&req, token.as_deref()) => {
                            (StatusCode::OK, Bytes::from(handle.render()))
                        }
                        "/metrics" => (StatusCode::UNAUTHORIZED, Bytes::new()),
                        _ => (StatusCode::NOT_FOUND, Bytes::new()),
                    };
                    let mut builder = Response::builder().status(status);
                    if status == StatusCode::UNAUTHORIZED {
                        builder = builder.header(WWW_AUTHENTICATE, "Bearer");
                    }
                    std::future::ready(builder.body(BodyFull::new(body)))
                });
                if let Err(error) = ServerBuilder::new(TokioExecutor::new())
                    .serve_connection(TokioIo::new(stream), service)
                    .await
                {
                    error!("failed to handle metrics request: {error}");
                }
            });
        }
    }))
}

fn is_authorized(req: &Request<BodyIncoming>, token: Option<&str>) -> bool {
    let Some(token) = token else {
        return true;
    };
    req.headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|value| tokens_match(value, token))
}
