use ndarray::{s, Array1,Array2};
use ndarray_npy::{read_npy,write_npy};
use qdrant_client::qdrant::{CountPointsBuilder,GetPointsBuilder,PointStruct,UpsertPointsBuilder};
use qdrant_client::qdrant::{Query, QueryPointsBuilder, QueryPoints, QueryBatchPointsBuilder};
use qdrant_client::{Payload};
use qdrant_client::Qdrant;
use std::time::Instant;
use std::env;
use std::fs::OpenOptions;
use std::sync::Arc;
use tokio::sync::Barrier;
use tokio::sync::Mutex;
use chrono::Utc;
use csv::WriterBuilder;
use tokio::time::{sleep, Duration};
use ndarray::{ArrayBase, OwnedRepr, Dim, Axis};
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::fmt::Write as _;
use tokio::sync::Notify;

#[derive(Debug)]
struct Endpoint {
    pub ip: String,
    pub port: u16,
}

/// Split `n_total` items into `parts` contiguous chunks as evenly as possible.
/// Returns (start, end) indices for chunk `part_idx` in half-open form [start, end).
///
/// Guarantees:
/// - All chunks are disjoint and cover [0, n_total)
/// - Chunk sizes differ by at most 1
/// - Earlier chunks get the +1 when there is a remainder
#[inline]
pub fn even_chunk(part_idx: usize, parts: usize, n_total: usize) -> (usize, usize) {
    assert!(parts > 0, "parts must be > 0");
    assert!(part_idx < parts, "part_idx out of range");

    let base = n_total / parts;
    let rem = n_total % parts;

    // All earlier parts contribute `base` items, plus 1 extra for each earlier part < rem.
    let start = part_idx * base + part_idx.min(rem);

    // This part gets 1 extra item if it's in the first `rem` parts.
    let extra = if part_idx < rem { 1 } else { 0 };

    let end = start + base + extra;
    (start, end)
}

/// Given a global `rank` in [0, n_workers*2), compute the row range [start,end)
/// assigned to that (worker, client) pair.
///
/// Splitting logic:
/// 1) Split all rows evenly across workers
/// 2) Split each worker's rows evenly across its 2 clients
#[inline]
pub fn range_for_rank(rank: usize, n_workers: usize, clients_per_worker: usize, n_rows_total: usize) -> (usize, usize) {
    assert!(n_workers > 0, "n_workers must be > 0");
    let world = n_workers * clients_per_worker;
    assert!(rank < world, "rank out of range");

    let worker_id = rank / clients_per_worker;
    let client_id = rank % clients_per_worker;

    // Worker chunk
    let (w_start, w_end) = even_chunk(worker_id, n_workers, n_rows_total);
    let w_len = w_end - w_start;

    // Client sub-chunk within worker
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

    parts.next(); // ignore first field

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


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    
    let N_WORKERS: usize = env::var("N_WORKERS")
        .expect("Environment variable N_WORKERS is missing" )
        .parse()
        .expect("N_WORKERS must be a valid integer");
    
    
    let CORPUS_SIZE: usize = env::var("CORPUS_SIZE")
        .expect("Environment variable CORPUS_SIZE is missing" )
        .parse()
        .expect("CORPUS_SIZE must be a valid integer");
    

    /// World ranks are laid out as:
    ///   rank = worker_id * CLIENTS_PER_WORKER + client_id
    let CLIENTS_PER_WORKER: usize = env::var("UPLOAD_CLIENTS_PER_WORKER")
        .expect("Environment variable UPLOAD_CLIENTS_PER_WORKER is missing" )
        .parse()
        .expect("UPLOAD_CLIENTS_PER_WORKER must be a valid integer");
    
    
    
    let npy_path = env::var("DATA_FILEPATH")
        .expect("Environment variable DATA_FILEPATH is missing");
    
   
    // Shared async barrier and lock
    let barrier = Arc::new(Barrier::new(CLIENTS_PER_WORKER * N_WORKERS));
    let lock = Arc::new(Mutex::new(0usize));

    let mut handles = Vec::new();


    let data: Array2<f32> = read_npy(&npy_path).expect("Failed to read npy file");
    let start = 0;
    
    let data = data.slice(s![start..CORPUS_SIZE, ..]).to_owned();
    let (n_rows_total, dim) = data.dim();
    println!("Main loaded {}, shape = {}x{}", npy_path, n_rows_total, dim);
    let shared_data = Arc::new(data);


    for rank in 0..(N_WORKERS * CLIENTS_PER_WORKER) {
        let barrier = barrier.clone();
        let lock = lock.clone();
        let data_slice = Arc::clone(&shared_data);

        let (start, end) = range_for_rank(rank, N_WORKERS, CLIENTS_PER_WORKER, n_rows_total);

        if start == end {
            return Ok::<_, anyhow::Error>(());
        }
        // println!("rank {} -> rows [{}, {})", rank, start, end);
        
        // Spawn one async "instance" per rank
        let handle = tokio::spawn(async move {
            worker(rank, CLIENTS_PER_WORKER, CLIENTS_PER_WORKER * N_WORKERS, data_slice, barrier, lock).await?;
            Ok::<_, anyhow::Error>(())
        });

        handles.push(handle);
    }

    // Wait for all workers to finish
    for handle in handles {
        handle.await??;
    }

    Ok(())
}


async fn worker(rank: usize, nClients: usize, world_size: usize, data_slice: Arc<Array2<f32>>, barrier: Arc<Barrier>,lock: Arc<Mutex<usize>>,) -> anyhow::Result<()> {
    
    let batch_size: usize = env::var("UPLOAD_BATCH_SIZE")
        .expect("Environment variable UPLOAD_BATCH_SIZE is missing" )
        .parse()
        .expect("UPLOAD_BATCH_SIZE must be a valid integer");
        
    // let qdrant_url = env::var("QDRANT_URL").unwrap_or_else(|_| "http://localhost:6334".to_string());
    let balance_strategy = env::var("UPLOAD_BALANCE_STRATEGY")
        .expect("Environment variable UPLOAD_BALANCE_STRATEGY is missing");



    let target = match balance_strategy.as_str() {
        "NO_BALANCE" => 0,
        "WORKER_BALANCE" => rank / nClients,
        _ => panic!("Unknown balance strategy"),
    };
    let endpoint = read_endpoint_line("ip_registry.txt", target + 1)?
        .expect("line not found");
    let qdrant_url = format!("http://{}:{}", endpoint.ip, endpoint.port - 1);
    

    
    let N_WORKERS: usize = env::var("N_WORKERS")
        .expect("Environment variable N_WORKERS is missing" )
        .parse()
        .expect("N_WORKERS must be a valid integer");
    let (n_rows_total, dim) = data_slice.dim();
    let (start_slice, end_slice) = range_for_rank(rank, N_WORKERS, nClients, n_rows_total);
    
    let view = data_slice.slice(s![start_slice..end_slice, ..]);


    

    // First row in this rank's slice
    let first = view.row(0);

    // Build a small preview (first 8 floats)
    let preview_n = 8.min(first.len());
    let mut preview = String::new();
    preview.push('[');
    for i in 0..preview_n {
        if i > 0 { preview.push_str(", "); }
        // limit decimals so logs are smaller
        let _ = write!(&mut preview, "{:.6}", first[i]);
    }
    if first.len() > preview_n {
        preview.push_str(", ...");
    }
    preview.push(']');

    // Also compute a simple checksum-ish value (cheap, stable-ish)
    let mut hash: u64 = 1469598103934665603; // FNV offset basis
    for &x in first.iter() {
        // hash the raw bits so tiny float diffs show up
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



    let (rows, dim) = view.dim();
   

    let client = Qdrant::from_url(&qdrant_url)
    .timeout(Duration::from_secs(9999))
    .build()?;
    let collection_name = "singleShard";
    let info = client.collection_info(collection_name).await?;
    if rank == 0 {
        println!("{:?}",info);
    }
    barrier.wait().await;
    // --- Batch upload ---
    let mut batch_idx = 0;
    
    let mut elapsed_process_times = Vec::new();
    let mut elapsed_upload_times = Vec::new();
    let mut elapsed_op_times = Vec::new();
    let mut elapsed_count_times = Vec::new();
    
    let offset = start_slice;
    println!("Rank {} connecting to Qdrant at {} with offset {}-{}, worker target {}, and batch size {}", rank, qdrant_url, offset, end_slice, target, batch_size);

    // wait for everyone to load in data 
    barrier.wait().await;

    let mut start_upload: Instant;
    let mut end_upload: Instant;


    let start = Utc::now();
    let start_loop = Instant::now();

    let run_window = Duration::from_secs(10 * 60);
    let sleep_window = Duration::from_secs(5 * 60);

    let mut cycle_start = Instant::now();
    for chunk in view.axis_chunks_iter(Axis(0), batch_size) {

        // if cycle_start.elapsed() >= run_window {
        //     println!("Rank {} 10 minutes elapsed — sleeping for 1 minute...", rank);
        //     sleep(sleep_window).await;
        //     cycle_start = Instant::now(); // reset cycle
        //  }
        // // println!("chunk shape: {:?}", chunk.dim());
        let start_batch = Instant::now();
        
        // create batch
        let points: Vec<PointStruct> = chunk
        .outer_iter() // iterate over rows
        .enumerate()
        .map(|(i, row)| {
            let id = ((batch_idx * batch_size) + i + offset) as u64;
            let vector = row.to_vec(); // convert ndarray row to Vec<f32>
            PointStruct::new(id, vector, Payload::default())
        })
        .collect();
        
        let batch = UpsertPointsBuilder::new(collection_name, points).wait(false);
        // let batch = UpsertPointsBuilder::new(collection_name, points).shard_key_selector((target).to_string());
        let start_upload = Instant::now();

        // upload batch
        client.upsert_points(batch).await?;
        
        let end_upload = Instant::now();
        
        // add iteration times to stack
        elapsed_process_times.push(start_upload.duration_since(start_batch).as_secs_f64());
        elapsed_upload_times.push(end_upload.duration_since(start_upload).as_secs_f64());
        elapsed_op_times.push(end_upload.duration_since(start_batch).as_secs_f64());

        batch_idx+= 1;
    }
    let end = Utc::now();
    let end_loop = Instant::now();
    let expected = (n_rows_total) as u64;
    
    // wait for everyone to finish insert requests
    barrier.wait().await;
    if rank == 0 {

        loop {
            let start_count = Instant::now();
            let count = client.count(CountPointsBuilder::new(collection_name).exact(true).timeout(9999)).await?.result.unwrap().count;
            let end_count = Instant::now();
            elapsed_count_times.push(end_count.duration_since(start_count).as_secs_f64());
            
            if count == expected {
                break count;
            }
            
            sleep(Duration::from_millis(100)).await;
        };
    }
    
    // wait until rank 0 has detected the expected count
    barrier.wait().await;
    let searchable = Instant::now();
    let global_end = Utc::now();
    
    let localExpected =  (end_slice) as u64;
    let resp = client.get_points(GetPointsBuilder::new(collection_name,vec![(localExpected - 1).into()])).await?.result;
    let exists = !resp.is_empty();
    let sanity_check = exists.to_string();
    
    // println!("{resp:?}");
    // println!("exists = {}", exists);
    let loop_duration = end_loop.duration_since(start_loop).as_secs_f64().to_string();
    let wait_period = searchable.duration_since(end_loop).as_secs_f64().to_string();
    let total = searchable.duration_since(start_loop).as_secs_f64().to_string();
    
    // this is dumb but it lets me order the writes without more work
        
    // safely write overall times to file
    {

        let _guard = lock.lock().await;
        let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open("times.csv")?;

        let mut w = WriterBuilder::new()
            .has_headers(false)   // don't rewrite headers every time
            .from_writer(file);

        
        if rank == 0 {
            w.write_record(&["rank","sanity_check", "loop_duration", "wait_period","total", "start_loop_utc", "end_loop_utc","global_end_utc"])?;

        }
        w.write_record(&[rank.to_string(), sanity_check, loop_duration, wait_period, total, start.to_rfc3339(), end.to_rfc3339(), global_end.to_rfc3339()])?;

    }


    let Bfilename = format!("batch_construction_times_rank_{}.npy", rank);
    let Ufilename = format!("upload_times_rank_{}.npy", rank);
    let Ofilename = format!("op_times_rank_{}.npy", rank);
    let Cfilename = format!("count_times_rank_{}.npy", rank);

    let elapsed_process_array = Array1::from(elapsed_process_times);
    let elapsed_upload_array = Array1::from(elapsed_upload_times);
    let elapsed_op_array = Array1::from(elapsed_op_times);
    let elapsed_count_array = Array1::from(elapsed_count_times);

    write_npy(&Bfilename, &elapsed_process_array).expect("Failed to save .npy file");
    write_npy(&Ufilename, &elapsed_upload_array).expect("Failed to save .npy file");
    write_npy(&Ofilename, &elapsed_op_array).expect("Failed to save .npy file");
    write_npy(&Cfilename, &elapsed_count_array).expect("Failed to save .npy file");
    
    println!("RANK {} BATCHES: {}", rank, batch_idx);
    Ok(())
}
