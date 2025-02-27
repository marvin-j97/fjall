use fjall::{Config, PersistMode};
use std::path::Path;
use std::sync::{
    atomic::{AtomicUsize, Ordering::Relaxed},
    Arc,
};

const PRODUCER_COUNT: usize = 4;
const PRODUCING_COUNT: usize = 100;

const EXPECTED_COUNT: usize = PRODUCER_COUNT * PRODUCING_COUNT;

fn main() -> fjall::Result<()> {
    let path = Path::new(".fjall_data");

    let keyspace = Config::new(path).temporary(true).open_transactional()?;
    let tasks = keyspace.open_partition("tasks", Default::default())?;

    let counter = Arc::new(AtomicUsize::default());

    let producers = (0..PRODUCER_COUNT)
        .map(|idx| {
            let keyspace = keyspace.clone();
            let tasks = tasks.clone();

            std::thread::spawn(move || {
                use rand::Rng;

                let mut rng = rand::thread_rng();

                for _ in 0..PRODUCING_COUNT {
                    let task_id = scru128::new_string();

                    tasks.insert(&task_id, &task_id)?;

                    println!("producer {idx} created task {task_id}");

                    let ms = rng.gen_range(10..100);
                    std::thread::sleep(std::time::Duration::from_millis(ms));
                }

                println!("producer {idx} done");

                Ok::<_, fjall::Error>(())
            })
        })
        .collect::<Vec<_>>();

    let consumers = (0..4)
        .map(|idx| {
            let keyspace = keyspace.clone();
            let tasks = tasks.clone();
            let counter = counter.clone();

            std::thread::spawn(move || {
                use rand::Rng;

                let mut rng = rand::thread_rng();

                loop {
                    let mut tx = keyspace.write_tx().unwrap();

                    // NOTE:
                    // Tombstones will add up over time with `remove`, making first KV slower
                    // and growing disk usage. `remove_weak` is used to prevent those
                    // tombstones from building up.
                    if let Some((key, _)) = tx.first_key_value(&tasks)? {
                        let task_id = std::str::from_utf8(&key).unwrap().to_owned();

                        tx.remove_weak(&tasks, key);

                        if tx.commit()?.is_ok() {
                            counter.fetch_add(1, Relaxed);
                        }

                        println!("consumer {idx} completed task {task_id}");

                        let ms = rng.gen_range(50..200);
                        std::thread::sleep(std::time::Duration::from_millis(ms));
                    } else if counter.load(Relaxed) == EXPECTED_COUNT {
                        return Ok::<_, fjall::Error>(());
                    }
                }
            })
        })
        .collect::<Vec<_>>();

    for t in producers {
        t.join().unwrap()?;
    }

    for t in consumers {
        t.join().unwrap()?;
    }

    assert_eq!(EXPECTED_COUNT, counter.load(Relaxed));

    Ok(())
}
