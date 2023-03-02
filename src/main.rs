use redis::{Client, Commands, RedisResult};
use std::collections::HashMap;
use std::time::Instant;

fn redis_set(
    data: &HashMap<String, String>,
    redis_client: &mut redis::Connection,
) -> RedisResult<usize> {
    let mut count = 0usize;
    for (key, value) in data.iter() {
        let _: () = redis_client.set(key, value)?;
        count += 1;
    }
    Ok(count)
}

fn redis_get(
    data: &HashMap<String, String>,
    redis_client: &mut redis::Connection,
) -> RedisResult<usize> {
    let mut count = 0usize;
    for key in data.keys() {
        let val: Option<String> = redis_client.get(key)?;
        if val.is_some() {
            count += 1;
        }
    }
    Ok(count)
}

fn run_tests(
    num: usize,
    tests: Vec<fn(&HashMap<String, String>, &mut redis::Connection) -> RedisResult<usize>>,
) -> RedisResult<()> {
    let mut data = HashMap::new();
    for i in 0..num {
        data.insert(format!("key{}", i), format!("val{}", i).repeat(100));
    }

    let client = Client::open("redis://127.0.0.1:6379")?;
    let mut conn = client.get_connection()?;

    let mut total_ops = 0usize;
    for test in tests.iter() {
        let start = Instant::now();
        let ops = test(&data, &mut conn)?;
        let elapsed_time = start.elapsed();
        let elapsed_time_secs =
            elapsed_time.as_secs() as f64 + elapsed_time.subsec_nanos() as f64 * 1e-9;
        let ops_per_sec = ops as f64 / elapsed_time_secs;
        total_ops += ops;
        println!(
            "{} elapsed time: {:.4} seconds, {} ops, {:.2} ops/sec, {} total ops",
            std::any::type_name::<
                fn(&HashMap<String, String>, &mut redis::Connection) -> RedisResult<usize>,
            >(),
            elapsed_time_secs,
            ops,
            ops_per_sec,
            total_ops
        );
    }
    Ok(())
}

fn main() -> RedisResult<()> {
    let num: usize = 100_000; // Change this to a larger number to see the difference
    let tests: Vec<fn(&HashMap<String, String>, &mut redis::Connection) -> RedisResult<usize>> =
        vec![redis_set, redis_get];
    run_tests(num, tests)?;
    Ok(())
}
