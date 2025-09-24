use rayon::{ThreadPool, ThreadPoolBuilder};
use std::thread;
use std::time::Duration;

#[test]
fn test_rotate_left_and_right() {
    let h: u32 = 12345678;
    let delta1 = (h >> 17) | (h << 15);
    println!("delta1:{}", delta1);

    let delta2 = h.rotate_right(17) | h.rotate_left(15);
    println!("delta1:{}", delta2);
}

#[test]
fn test_div_ceil() {
    let nbits: u32 = 0;
    let nbytes1 = (nbits + 7) / 8;

    println!("nbytes:{}", nbytes1);

    let nbytes2 = nbits.div_ceil(8);
    println!("nbytes:{}", nbytes2);
}

#[test]
fn test_thread_pool() {
    let db = Database::new(4);
    db.do_compact("t1");
    db.do_compact("t2");
    db.do_compact("L0");
    db.do_compact("L1");
    println!("Rayon 任务已提交。");

    // db 被 drop 时，其内部的 rayon::ThreadPool 也会被 drop，
    // 这会阻塞并等待池中所有任务完成。
    println!("准备关闭基于 Rayon 的数据库...");
}

struct Database {
    pool: ThreadPool,
}

impl Database {
    fn new(num_threads: usize) -> Self {
        let pool = ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build()
            .unwrap();
        Self { pool }
    }

    fn do_flush(&self, table_name: String) {
        self.pool.spawn(move || {
            println!("开始 Flush: {}", table_name);
            thread::sleep(Duration::from_secs(2)); // 模拟耗时操作
            println!("完成 Flush: {}", table_name);
        });
    }

    fn do_compact(&self, level: &str) {
        let level_name = level.to_string();
        self.pool.install(move || {
            println!("Rayon 开始 Compact: {}", level_name);
            thread::sleep(Duration::from_secs(3));
            println!("Rayon 完成 Compact: {}", level_name);
        });
    }
}
