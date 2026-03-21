use anyhow::{bail, Context};
use chrono::Utc;
use csv::WriterBuilder;
use ndarray::{s, Array1, Array2, ArrayView2, Axis};
use ndarray_npy::{read_npy, write_npy};
use qdrant_client::qdrant::{
    CountPointsBuilder, GetPointsBuilder, PointStruct, Query, QueryBatchPointsBuilder,
    QueryPoints, QueryPointsBuilder, UpsertPointsBuilder,
};
use qdrant_client::{Payload, Qdrant};
use std::env;
use std::fmt::Write as _;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{Barrier, Mutex};
use tokio::time::{sleep, Duration};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ActiveTask {
    Upload,
    Query,
}

impl ActiveTask {
    fn as_env_prefix(self) -> &'static str {
        match self {
            Self::Upload => "INSERT",
            Self::Query => "QUERY",
        }
    }

    fn as_file_prefix(self) -> &'static str {
        match self {
            Self::Upload => "insert",
            Self::Query => "query",
        }
    }
}

#[derive(Clone, Debug)]
struct RunConfig {
    active_task: ActiveTask,
    n_workers: usize,
    clients_per_worker: usize,
    corpus_size: usize,
    batch_size: usize,
    balance_strategy: String,
    npy_path: String,
    debug_results: bool,
}

#[derive(Debug)]
struct Endpoint {
    ip: String,
    port: u16,
}

#[inline]
pub fn even_chunk(part_idx: usize, parts: usize, n_total: usize) -> (usize, usize) {
    assert!(parts > 0, "parts must be > 0");
    assert!(part_idx < parts, "part_idx out of range");

    let base = n_total / parts;
    let rem = n_total % parts;
    let start = part_idx * base + part_idx.min(rem);
    let extra = if part_idx < rem { 1 } else { 0 };
    let end = start + base + extra;
    (start, end)
}

#[inline]
pub fn range_for_rank(
    rank: usize,
    n_workers: usize,
    clients_per_worker: usize,
    n_rows_total: usize,
) -> (usize, usize) {
    assert!(n_workers > 0, "n_workers must be > 0");
    let world = n_workers * clients_per_worker;
    assert!(rank < world, "rank out of range");

    let worker_id = rank / clients_per_worker;
    let client_id = rank % clients_per_worker;

    let (w_start, w_end) = even_chunk(worker_id, n_workers, n_rows_total);
    let w_len = w_end - w_start;
    let (c_off_start, c_off_end) = even_chunk(client_id, clients_per_worker, w_len);

    let start = w_start + c_off_start;
    let end = w_start + c_off_end;
    (start, end)
}

fn read_endpoint_line(path: &str, line_num: usize) -> io::Result<Option<Endpoint>> {
    let reader = BufReader::new(File::open(path)?);

    let line = match reader.lines().nth(line_num - 1) {
        Some(line) => line?,
        None => return Ok(None),
    };

    let mut parts = line.split(',');
    parts.next();

    let ip = parts
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing IP"))?
        .to_string();

    let port: u16 = parts
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing port"))?
        .parse()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid port"))?;

    Ok(Some(Endpoint { ip, port }))
}

fn env_var(name: &str) -> Option<String> {
    env::var(name).ok().map(|value| value.trim().to_string())
}

fn first_env(names: &[&str]) -> Option<String> {
    names.iter().find_map(|name| env_var(name).filter(|value| !value.is_empty()))
}

fn parse_required_usize(names: &[&str], label: &str) -> anyhow::Result<usize> {
    let raw = first_env(names)
        .with_context(|| format!("missing environment variable for {label}: tried {:?}", names))?;
    raw.parse()
        .with_context(|| format!("{label} must be a valid integer, got {raw:?}"))
}

fn parse_optional_bool(names: &[&str]) -> bool {
    first_env(names)
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "True" | "yes" | "YES"))
        .unwrap_or(false)
}

fn infer_active_task() -> anyhow::Result<ActiveTask> {
    let active_task = first_env(&["ACTIVE_TASK", "TASK"])
        .context("missing ACTIVE_TASK/TASK; set ACTIVE_TASK=INSERT or ACTIVE_TASK=QUERY")?;
    parse_active_task(&active_task)
}

fn parse_active_task(raw: &str) -> anyhow::Result<ActiveTask> {
    match raw.trim().to_ascii_uppercase().as_str() {
        "INSERT" => Ok(ActiveTask::Upload),
        "QUERY" => Ok(ActiveTask::Query),
        other => bail!("unsupported ACTIVE_TASK/TASK value: {other} (expected INSERT or QUERY)"),
    }
}

fn load_config() -> anyhow::Result<RunConfig> {
    let active_task = infer_active_task()?;
    let n_workers = parse_required_usize(&["N_WORKERS"], "N_WORKERS")?;

    let clients_key = format!("{}_CLIENTS_PER_WORKER", active_task.as_env_prefix());
    let batch_key = format!("{}_BATCH_SIZE", active_task.as_env_prefix());
    let balance_key = format!("{}_BALANCE_STRATEGY", active_task.as_env_prefix());

    let clients_per_worker = parse_required_usize(&[clients_key.as_str()], "clients_per_worker")?;
    let corpus_size = match active_task {
        ActiveTask::Upload => {
            parse_required_usize(&["INSERT_CORPUS_SIZE"], "insert corpus size")?
        }
        ActiveTask::Query => {
            parse_required_usize(&["QUERY_CORPUS_SIZE"], "query corpus size")?
        }
    };
    let batch_size = parse_required_usize(&[batch_key.as_str()], "batch_size")?;

    let balance_strategy =
        first_env(&[balance_key.as_str()]).with_context(|| format!("missing {balance_key}"))?;

    let npy_path = match active_task {
        ActiveTask::Upload => first_env(&["INSERT_FILEPATH"])
            .context("missing insert filepath env: tried INSERT_FILEPATH")?,
        ActiveTask::Query => first_env(&["QUERY_FILEPATH"])
            .context("missing query filepath env: tried QUERY_FILEPATH")?,
    };

    let debug_results = matches!(active_task, ActiveTask::Query)
        && parse_optional_bool(&["QUERY_DEBUG_RESULTS"]);

    Ok(RunConfig {
        active_task,
        n_workers,
        clients_per_worker,
        corpus_size,
        batch_size,
        balance_strategy,
        npy_path,
        debug_results,
    })
}

fn resolve_qdrant_url(
    rank: usize,
    clients_per_worker: usize,
    balance_strategy: &str,
) -> anyhow::Result<(usize, String)> {
    let target = match balance_strategy {
        "NO_BALANCE" => 0,
        "WORKER_BALANCE" => rank / clients_per_worker,
        _ => bail!("unknown balance strategy: {balance_strategy}"),
    };

    let endpoint = read_endpoint_line("ip_registry.txt", target + 1)?
        .with_context(|| format!("ip_registry.txt missing line {}", target + 1))?;
    let qdrant_url = format!("http://{}:{}", endpoint.ip, endpoint.port - 1);
    Ok((target, qdrant_url))
}

fn log_rank_preview(rank: usize, start_slice: usize, end_slice: usize, view: ArrayView2<'_, f32>) {
    let first = view.row(0);
    let preview_n = 8.min(first.len());
    let mut preview = String::new();
    preview.push('[');
    for i in 0..preview_n {
        if i > 0 {
            preview.push_str(", ");
        }
        let _ = write!(&mut preview, "{:.6}", first[i]);
    }
    if first.len() > preview_n {
        preview.push_str(", ...");
    }
    preview.push(']');

    let mut hash: u64 = 1469598103934665603;
    for &x in first {
        hash ^= x.to_bits() as u64;
        hash = hash.wrapping_mul(1099511628211);
    }

    println!(
        "rank {} slice [{}, {}) len={} first_row_preview={} first_row_hash=0x{:016x}",
        rank,
        start_slice,
        end_slice,
        end_slice - start_slice,
        preview,
        hash
    );
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Arc::new(load_config()?);
    let barrier = Arc::new(Barrier::new(config.n_workers * config.clients_per_worker));
    let lock = Arc::new(Mutex::new(()));
    let mut handles = Vec::new();

    let data: Array2<f32> = read_npy(&config.npy_path).context("failed to read npy file")?;
    let data = data.slice(s![0..config.corpus_size, ..]).to_owned();
    let (n_rows_total, dim) = data.dim();
    println!(
        "ACTIVE_TASK={} loaded {}, shape = {}x{}",
        config.active_task.as_env_prefix(),
        config.npy_path,
        n_rows_total,
        dim
    );
    let shared_data = Arc::new(data);

    for rank in 0..(config.n_workers * config.clients_per_worker) {
        let barrier = Arc::clone(&barrier);
        let lock = Arc::clone(&lock);
        let data_slice = Arc::clone(&shared_data);
        let config = Arc::clone(&config);

        let (start, end) = range_for_rank(
            rank,
            config.n_workers,
            config.clients_per_worker,
            n_rows_total,
        );
        if start == end {
            continue;
        }

        let handle = tokio::spawn(async move {
            worker(rank, config, data_slice, barrier, lock).await?;
            Ok::<_, anyhow::Error>(())
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await??;
    }

    Ok(())
}

async fn worker(
    rank: usize,
    config: Arc<RunConfig>,
    data_slice: Arc<Array2<f32>>,
    barrier: Arc<Barrier>,
    lock: Arc<Mutex<()>>,
) -> anyhow::Result<()> {
    let (target, qdrant_url) =
        resolve_qdrant_url(rank, config.clients_per_worker, &config.balance_strategy)?;
    let (n_rows_total, _) = data_slice.dim();
    let (start_slice, end_slice) = range_for_rank(
        rank,
        config.n_workers,
        config.clients_per_worker,
        n_rows_total,
    );
    let view = data_slice.slice(s![start_slice..end_slice, ..]);

    log_rank_preview(rank, start_slice, end_slice, view.view());

    let client = Qdrant::from_url(&qdrant_url)
        .timeout(Duration::from_secs(9999))
        .build()?;
    let collection_name = "singleShard";
    let info = client.collection_info(collection_name).await?;
    if rank == 0 {
        println!("{info:?}");
    }

    println!(
        "Rank {} task={} connecting to Qdrant at {} with offset {}-{}, worker target {}, and batch size {}",
        rank,
        config.active_task.as_env_prefix(),
        qdrant_url,
        start_slice,
        end_slice,
        target,
        config.batch_size
    );

    match config.active_task {
        ActiveTask::Upload => {
            run_upload(
                rank,
                &client,
                collection_name,
                view,
                start_slice,
                n_rows_total,
                config.batch_size,
                barrier,
                lock,
                config.active_task,
            )
            .await
        }
        ActiveTask::Query => {
            run_query(
                rank,
                &client,
                collection_name,
                view,
                config.batch_size,
                config.debug_results,
                barrier,
                lock,
                config.active_task,
            )
            .await
        }
    }
}

struct TimeFiles {
    csv: String,
    batch_npy: String,
    main_npy: String,
    op_npy: String,
    extra_npy: Option<String>,
}

fn time_files(task: ActiveTask) -> TimeFiles {
    let prefix = task.as_file_prefix();
    match task {
        ActiveTask::Upload => TimeFiles {
            csv: format!("{prefix}_times.csv"),
            batch_npy: format!("{prefix}_batch_construction_times"),
            main_npy: format!("{prefix}_upload_times"),
            op_npy: format!("{prefix}_op_times"),
            extra_npy: Some(format!("{prefix}_count_times")),
        },
        ActiveTask::Query => TimeFiles {
            csv: format!("{prefix}_times.csv"),
            batch_npy: format!("{prefix}_batch_construction_times"),
            main_npy: format!("{prefix}_query_times"),
            op_npy: format!("{prefix}_op_times"),
            extra_npy: None,
        },
    }
}

async fn run_upload(
    rank: usize,
    client: &Qdrant,
    collection_name: &str,
    view: ArrayView2<'_, f32>,
    offset: usize,
    n_rows_total: usize,
    batch_size: usize,
    barrier: Arc<Barrier>,
    lock: Arc<Mutex<()>>,
    task: ActiveTask,
) -> anyhow::Result<()> {
    barrier.wait().await;

    let mut batch_idx = 0;
    let mut elapsed_process_times = Vec::new();
    let mut elapsed_upload_times = Vec::new();
    let mut elapsed_op_times = Vec::new();
    let mut elapsed_count_times = Vec::new();

    if rank == 0 {
        File::create("./perf/workflow_start.txt")?;
        sleep(Duration::from_secs(3)).await;
    }
    barrier.wait().await;

    let start = Utc::now();
    let start_loop = Instant::now();

    for chunk in view.axis_chunks_iter(Axis(0), batch_size) {
        let start_batch = Instant::now();
        let points: Vec<PointStruct> = chunk
            .outer_iter()
            .enumerate()
            .map(|(i, row)| {
                let id = ((batch_idx * batch_size) + i + offset) as u64;
                PointStruct::new(id, row.to_vec(), Payload::default())
            })
            .collect();

        let batch = UpsertPointsBuilder::new(collection_name, points).wait(false);
        let start_upload = Instant::now();
        client.upsert_points(batch).await?;
        let end_upload = Instant::now();

        elapsed_process_times.push(start_upload.duration_since(start_batch).as_secs_f64());
        elapsed_upload_times.push(end_upload.duration_since(start_upload).as_secs_f64());
        elapsed_op_times.push(end_upload.duration_since(start_batch).as_secs_f64());
        batch_idx += 1;
    }

    let end = Utc::now();
    let end_loop = Instant::now();
    let expected = n_rows_total as u64;

    barrier.wait().await;
    if rank == 0 {
        loop {
            let start_count = Instant::now();
            let count = client
                .count(
                    CountPointsBuilder::new(collection_name)
                        .exact(true)
                        .timeout(9999),
                )
                .await?
                .result
                .context("count response missing result")?
                .count;
            let end_count = Instant::now();
            elapsed_count_times.push(end_count.duration_since(start_count).as_secs_f64());

            if count == expected {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }
    }

    barrier.wait().await;
    let searchable = Instant::now();
    let global_end = Utc::now();

    if rank == 0 {
        File::create("./perf/workflow_stop.txt")?;
        sleep(Duration::from_secs(3)).await;
    }

    let point_id = (offset + view.dim().0 - 1) as u64;
    let resp = client
        .get_points(GetPointsBuilder::new(collection_name, vec![point_id.into()]))
        .await?
        .result;
    let sanity_check = (!resp.is_empty()).to_string();

    let files = time_files(task);
    write_upload_summary(
        rank,
        &files.csv,
        &lock,
        sanity_check,
        start,
        end,
        global_end,
        start_loop,
        end_loop,
        searchable,
    )
    .await?;

    write_npy(
        &format!("{}_rank_{}.npy", files.batch_npy, rank),
        &Array1::from(elapsed_process_times),
    )?;
    write_npy(
        &format!("{}_rank_{}.npy", files.main_npy, rank),
        &Array1::from(elapsed_upload_times),
    )?;
    write_npy(
        &format!("{}_rank_{}.npy", files.op_npy, rank),
        &Array1::from(elapsed_op_times),
    )?;
    if let Some(extra_prefix) = files.extra_npy {
        write_npy(
            &format!("{}_rank_{}.npy", extra_prefix, rank),
            &Array1::from(elapsed_count_times),
        )?;
    }

    println!("RANK {} BATCHES: {}", rank, batch_idx);
    Ok(())
}

async fn write_upload_summary(
    rank: usize,
    csv_path: &str,
    lock: &Mutex<()>,
    sanity_check: String,
    start: chrono::DateTime<Utc>,
    end: chrono::DateTime<Utc>,
    global_end: chrono::DateTime<Utc>,
    start_loop: Instant,
    end_loop: Instant,
    searchable: Instant,
) -> anyhow::Result<()> {
    let loop_duration = end_loop.duration_since(start_loop).as_secs_f64().to_string();
    let wait_period = searchable.duration_since(end_loop).as_secs_f64().to_string();
    let total = searchable.duration_since(start_loop).as_secs_f64().to_string();

    let _guard = lock.lock().await;
    let file = OpenOptions::new().create(true).append(true).open(csv_path)?;
    let mut w = WriterBuilder::new().has_headers(false).from_writer(file);

    if rank == 0 {
        w.write_record([
            "rank",
            "sanity_check",
            "loop_duration",
            "wait_period",
            "total",
            "start_loop_utc",
            "end_loop_utc",
            "global_end_utc",
        ])?;
    }

    w.write_record([
        rank.to_string(),
        sanity_check,
        loop_duration,
        wait_period,
        total,
        start.to_rfc3339(),
        end.to_rfc3339(),
        global_end.to_rfc3339(),
    ])?;
    Ok(())
}

async fn run_query(
    rank: usize,
    client: &Qdrant,
    collection_name: &str,
    view: ArrayView2<'_, f32>,
    batch_size: usize,
    debug_results: bool,
    barrier: Arc<Barrier>,
    lock: Arc<Mutex<()>>,
    task: ActiveTask,
) -> anyhow::Result<()> {
    barrier.wait().await;

    let mut batch_idx = 0;
    let mut elapsed_process_times = Vec::new();
    let mut elapsed_query_times = Vec::new();
    let mut elapsed_op_times = Vec::new();
    let mut printed_debug_results = false;

    barrier.wait().await;

    let start = Utc::now();
    let start_loop = Instant::now();

    for chunk in view.axis_chunks_iter(Axis(0), batch_size) {
        let start_batch = Instant::now();

        if batch_size == 1 {
            let query = QueryPointsBuilder::new(collection_name)
                .query(Query::new_nearest(chunk.row(0).to_vec()))
                .limit(10);
            let start_query = Instant::now();
            let response = client.query(query).await?;
            let end_query = Instant::now();

            if debug_results && rank == 0 && !printed_debug_results {
                println!(
                    "DEBUG rank {rank}: single query returned {} points",
                    response.result.len()
                );
                for (idx, point) in response.result.iter().take(3).enumerate() {
                    println!(
                        "DEBUG rank {rank}: result[{idx}] id={:?} score={:?} payload={:?}",
                        point.id, point.score, point.payload
                    );
                }
                printed_debug_results = true;
            }

            elapsed_process_times.push(start_query.duration_since(start_batch).as_secs_f64());
            elapsed_query_times.push(end_query.duration_since(start_query).as_secs_f64());
            elapsed_op_times.push(end_query.duration_since(start_batch).as_secs_f64());
        } else {
            let queries: Vec<QueryPoints> = chunk
                .outer_iter()
                .map(|row| {
                    QueryPointsBuilder::new(collection_name)
                        .query(Query::new_nearest(row.to_vec()))
                        .limit(10)
                        .build()
                })
                .collect();

            let batch_query = QueryBatchPointsBuilder::new(collection_name, queries)
                .timeout(999)
                .build();
            let start_query = Instant::now();
            let response = client.query_batch(batch_query).await?;
            let end_query = Instant::now();

            if debug_results && rank == 0 && !printed_debug_results {
                println!(
                    "DEBUG rank {rank}: batch query returned {} result sets",
                    response.result.len()
                );
                if let Some(first_query) = response.result.first() {
                    println!(
                        "DEBUG rank {rank}: first query returned {} points",
                        first_query.result.len()
                    );
                    for (idx, point) in first_query.result.iter().take(3).enumerate() {
                        println!(
                            "DEBUG rank {rank}: first_query.result[{idx}] id={:?} score={:?} payload={:?}",
                            point.id, point.score, point.payload
                        );
                    }
                }
                printed_debug_results = true;
            }

            elapsed_process_times.push(start_query.duration_since(start_batch).as_secs_f64());
            elapsed_query_times.push(end_query.duration_since(start_query).as_secs_f64());
            elapsed_op_times.push(end_query.duration_since(start_batch).as_secs_f64());
        }

        batch_idx += 1;
    }

    let end = Utc::now();
    let end_loop = Instant::now();
    barrier.wait().await;
    let searchable = Instant::now();
    let global_end = Utc::now();

    let files = time_files(task);
    write_query_summary(
        rank,
        &files.csv,
        &lock,
        start,
        end,
        global_end,
        start_loop,
        end_loop,
        searchable,
    )
    .await?;

    write_npy(
        &format!("{}_rank_{}.npy", files.batch_npy, rank),
        &Array1::from(elapsed_process_times),
    )?;
    write_npy(
        &format!("{}_rank_{}.npy", files.main_npy, rank),
        &Array1::from(elapsed_query_times),
    )?;
    write_npy(
        &format!("{}_rank_{}.npy", files.op_npy, rank),
        &Array1::from(elapsed_op_times),
    )?;

    println!("RANK {} BATCHES: {}", rank, batch_idx);
    Ok(())
}

async fn write_query_summary(
    rank: usize,
    csv_path: &str,
    lock: &Mutex<()>,
    start: chrono::DateTime<Utc>,
    end: chrono::DateTime<Utc>,
    global_end: chrono::DateTime<Utc>,
    start_loop: Instant,
    end_loop: Instant,
    searchable: Instant,
) -> anyhow::Result<()> {
    let loop_duration = end_loop.duration_since(start_loop).as_secs_f64().to_string();
    let wait_period = searchable.duration_since(end_loop).as_secs_f64().to_string();
    let total = searchable.duration_since(start_loop).as_secs_f64().to_string();

    let _guard = lock.lock().await;
    let file = OpenOptions::new().create(true).append(true).open(csv_path)?;
    let mut w = WriterBuilder::new().has_headers(false).from_writer(file);

    if rank == 0 {
        w.write_record([
            "rank",
            "loop_duration",
            "wait_period",
            "total",
            "start_loop_utc",
            "end_loop_utc",
            "global_end_utc",
        ])?;
    }

    w.write_record([
        rank.to_string(),
        loop_duration,
        wait_period,
        total,
        start.to_rfc3339(),
        end.to_rfc3339(),
        global_end.to_rfc3339(),
    ])?;
    Ok(())
}
