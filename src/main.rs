//! Reproducer: DataFusion join operators don't yield, preventing query cancellation
//!
//! Run: `cargo test`
//!
//! Expected result: Tests pass - joins respond to cancellation
//! Actual result: Tests panic after 5s timeout - joins ignored cancellation

fn main() {
    println!("Run: cargo test");
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::*;

    /// How long to wait for cancellation before panicking
    const TIMEOUT: Duration = Duration::from_secs(5);

    async fn create_context(rows: i64) -> SessionContext {
        create_context_with_config(rows, SessionConfig::new()).await
    }

    async fn create_context_with_config(rows: i64, config: SessionConfig) -> SessionContext {
        let ctx = SessionContext::new_with_config(config);
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from((0..rows).map(|i| i % 2).collect::<Vec<_>>())),
                Arc::new(Int64Array::from((0..rows).collect::<Vec<_>>())),
            ],
        )
        .unwrap();
        ctx.register_batch("t1", batch.clone()).unwrap();
        ctx.register_batch("t2", batch).unwrap();
        ctx
    }

    /// Test if a join query responds to cancellation.
    /// Uses std::thread primitives for timing to avoid tokio scheduler issues.
    async fn test_cancellation(ctx: &SessionContext, query: &str, join_exec: &str) {
        println!("Testing {join_exec} cancellation...");

        let plan = ctx.sql(query).await.unwrap().create_physical_plan().await.unwrap();
        // Verify the physical plan contains the expected join type
        assert!(format!("{:?}", plan).contains(join_exec), "Plan missing {join_exec}");

        let task_ctx = ctx.task_ctx();
        let handle = tokio::spawn(async move {
            datafusion::physical_plan::collect(plan, task_ctx).await
        });

        // Brief delay to ensure task starts executing the join
        std::thread::sleep(Duration::from_millis(250));

        // Request tokio task cancellation
        handle.abort();

        // Poll completion using OS-level sleep to bypass tokio worker thread starvation
        let start = Instant::now();
        while !handle.is_finished() {
            let elapsed = start.elapsed();
            if start.elapsed() > TIMEOUT {
                panic!(
                    "FAILED: {join_exec} did not respond to cancellation within {elapsed:?}\n\
                     The join operator is not yielding to the tokio runtime.",
                );
            }
            std::thread::sleep(Duration::from_millis(100));
        }

        let elapsed = start.elapsed();
        println!("PASSED: {join_exec} cancelled in {elapsed:?}");
    }

    /// Wrapper that creates a runtime, runs the test, and forces shutdown of the runtime on
    /// completion.
    ///
    /// We can't use #[tokio::test] because dropping the runtime waits for all tasks to complete,
    /// which would hang the test. By using shutdown_timeout(), we forcibly terminate tasks.
    fn run_test<F: std::future::Future<Output = ()> + Send + 'static>(f: F) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            rt.block_on(f);
        }));

        // Always force shutdown, even if test panicked
        rt.shutdown_timeout(Duration::from_millis(500));

        if let Err(e) = result {
            std::panic::resume_unwind(e);
        }
    }

    #[test]
    fn test_cross_join() {
        run_test(async {
            // 100K x 100K = 10B output rows
            let ctx = create_context(100_000).await;
            test_cancellation(&ctx, "SELECT SUM(t1.v + t2.v) FROM t1, t2", "CrossJoinExec").await;
        });
    }

    #[test]
    fn test_hash_join() {
        run_test(async {
            // 500K rows, key has 2 values -> 250K x 250K matches per key = 125B comparisons
            let ctx = create_context(500_000).await;
            test_cancellation(
                &ctx,
                "SELECT SUM(t1.v + t2.v) FROM t1 JOIN t2 ON t1.k = t2.k",
                "HashJoinExec",
            )
            .await;
        });
    }

    #[test]
    fn test_nested_loop_join() {
        run_test(async {
            // 100K x 100K with inequality predicate
            let ctx = create_context(100_000).await;
            test_cancellation(
                &ctx,
                "SELECT SUM(t1.v + t2.v) FROM t1 JOIN t2 ON t1.k < t2.k",
                "NestedLoopJoinExec",
            )
            .await;
        });
    }

    #[test]
    fn test_sort_merge_join() {
        run_test(async {
            let config =
                SessionConfig::new().set_bool("datafusion.optimizer.prefer_hash_join", false);
            // 500K rows with sort-merge
            let ctx = create_context_with_config(500_000, config).await;
            test_cancellation(
                &ctx,
                "SELECT SUM(t1.v + t2.v) FROM t1 JOIN t2 ON t1.k = t2.k",
                "SortMergeJoinExec",
            )
            .await;
        });
    }
}
