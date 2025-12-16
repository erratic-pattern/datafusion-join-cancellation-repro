# DataFusion Join Cancellation Bug Reproducer

Related Issue: https://github.com/apache/datafusion/issues/19358

Demonstrates that DataFusion (<=51.0.0) join operators (HashJoin, SortMergeJoin, NestedLoopJoin, CrossJoin) don't yield to the Tokio runtime, making long-running join queries uncancellable.

## Run

```bash
cargo test
```

## Output with DataFusion 51.0.0

```
running 4 tests
test tests::test_hash_join ... FAILED
test tests::test_cross_join ... FAILED
test tests::test_sort_merge_join ... FAILED
test tests::test_nested_loop_join ... FAILED

failures:

---- tests::test_hash_join stdout ----
Testing HashJoinExec cancellation...

thread 'tests::test_hash_join' (203017447) panicked at src/main.rs:72:17:
FAILED: HashJoinExec did not respond to cancellation within 5.000818042s
The join operator is not yielding to the tokio runtime.
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace

---- tests::test_cross_join stdout ----
Testing CrossJoinExec cancellation...

thread 'tests::test_cross_join' (203017446) panicked at src/main.rs:72:17:
FAILED: CrossJoinExec did not respond to cancellation within 5.001540333s
The join operator is not yielding to the tokio runtime.

---- tests::test_sort_merge_join stdout ----
Testing SortMergeJoinExec cancellation...

thread 'tests::test_sort_merge_join' (203017449) panicked at src/main.rs:72:17:
FAILED: SortMergeJoinExec did not respond to cancellation within 5.001545542s
The join operator is not yielding to the tokio runtime.

---- tests::test_nested_loop_join stdout ----
Testing NestedLoopJoinExec cancellation...

thread 'tests::test_nested_loop_join' (203017448) panicked at src/main.rs:72:17:
FAILED: NestedLoopJoinExec did not respond to cancellation within 5.085809791s
The join operator is not yielding to the tokio runtime.


failures:
    tests::test_cross_join
    tests::test_hash_join
    tests::test_nested_loop_join
    tests::test_sort_merge_join

test result: FAILED. 0 passed; 4 failed; 0 ignored; 0 measured; 0 filtered out; finished in 5.87s
```

**Note**: Results may vary depending on system resources. You may need to adjust timeouts and table sizes to reproduce.


## Output with `make_cooperative` fix 

```
running 4 tests
test tests::test_cross_join ... ok
test tests::test_sort_merge_join ... ok
test tests::test_hash_join ... ok
test tests::test_nested_loop_join ... ok

test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.56s
```

