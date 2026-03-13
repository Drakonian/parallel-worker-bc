# Test Plan for Parallel Worker

## Testability Notes

| Constraint | Impact |
|---|---|
| `Commit()` inside `RunFor*` methods | Tests that call `RunFor*` must clean up via `Coordinator.Cleanup()` since `AutoRollback` cannot undo committed data |
| `PW Task Dispatcher` / `PW Worker Runner` are `Internal` | Cannot call directly — tested indirectly through end-to-end integration tests |

**Two strategies used:**

1. **Unit tests** — Manually create `PW Batch` / `PW Batch Chunk` records to simulate states, then test consumer-facing logic against those records.
2. **Integration tests** — Call `RunForList` / `RunForRecords` / `RunForChunks` end-to-end with real background sessions, verify results, clean up after.

---

## 1. PW Chunk Context (unit tests)

Tests the input/output API that workers interact with.

| # | Scenario | What we verify |
|---|---|---|
| 1.1 | Init from chunk with JSON payload | `GetBatchId()`, `GetChunkIndex()` return correct values |
| 1.2 | `GetTextInput` reads text value | Returns correct text from payload |
| 1.3 | `GetIntInput` reads integer value | Returns correct integer from payload |
| 1.4 | `GetDecimalInput` reads decimal value | Returns correct decimal from payload |
| 1.5 | `GetBoolInput` reads boolean value | Returns correct boolean from payload |
| 1.6 | `GetInputArray` reads JSON array | Returns array with correct items |
| 1.7 | `GetInput` returns full JSON object | All keys present in returned object |
| 1.8 | `GetTextInput` with missing key | Errors with `Key "X" not found in chunk input.` |
| 1.9 | `GetIntInput` with missing key | Same error pattern |
| 1.10 | Any getter called before `Init` | Errors with `Chunk context has not been initialized.` |
| 1.11 | `IsRecordChunk` returns true for record payload | Payload contains `$IsRecordChunk` |
| 1.12 | `IsRecordChunk` returns false for list payload | Payload has `$Items` but no `$IsRecordChunk` |
| 1.13 | `GetChunkSize` computes `$EndIndex - $StartIndex + 1` | Correct count returned |
| 1.14 | `GetRecordRef` on non-record chunk | Errors with `This chunk was not created via RunForRecords.` |
| 1.15 | `SetResult` stores single result | After `SaveResultsTo`, blob contains JSON array with 1 object |
| 1.16 | `SetResult` replaces previous result | Call twice — blob contains only the last object |
| 1.17 | `AppendResult` accumulates results | Call 3 times — blob contains array of 3 objects |
| 1.18 | `SetResult` then `AppendResult` | Array contains 2 objects |
| 1.19 | `SaveResultsTo` with no results | Blob stays empty (no write) |

---

## 2. PW Batch Coordinator — Status & Query Methods

Manually create batch + chunk records in various states, then test coordinator's read methods.

| # | Scenario | What we verify |
|---|---|---|
| 2.1 | `GetStatus` on Running batch | Returns `Running` |
| 2.2 | `GetStatus` on Completed batch | Returns `Completed` |
| 2.3 | `GetStatus` on Failed batch | Returns `Failed` |
| 2.4 | `GetStatus` on PartialFailure batch | Returns `PartialFailure` |
| 2.5 | `GetStatus` with non-existent BatchId | Returns `Failed` (fallback) |
| 2.6 | `GetCompletedChunks` returns correct count | Create batch with `Completed Chunks = 3` |
| 2.7 | `GetTotalChunks` returns correct count | Create batch with `Total Chunks = 5` |
| 2.8 | `IsFinished` for `Completed` | Returns true |
| 2.9 | `IsFinished` for `Failed` | Returns true |
| 2.10 | `IsFinished` for `PartialFailure` | Returns true |
| 2.11 | `IsFinished` for `Running` | Returns false |
| 2.12 | `IsFinished` for `Pending` | Returns false |
| 2.13 | `IsFinished` with non-existent BatchId | Returns true |
| 2.14 | `WaitForCompletion` on already-Completed batch | Returns true immediately |
| 2.15 | `WaitForCompletion` on already-Failed batch | Returns false immediately |
| 2.16 | `WaitForCompletion` with null GUID | Returns true |

---

## 3. PW Batch Coordinator — Result Collection

Create chunks with pre-filled result/error blobs, then test collection methods.

| # | Scenario | What we verify |
|---|---|---|
| 3.1 | `GetResults` — single completed chunk with 1 result | Returns list with 1 JsonObject |
| 3.2 | `GetResults` — multiple completed chunks | Results from all chunks aggregated |
| 3.3 | `GetResults` — chunk with `AppendResult` (multiple results in one chunk) | All objects flattened into list |
| 3.4 | `GetResults` — skips failed chunks | Only completed chunk results returned |
| 3.5 | `GetResults` — no completed chunks | Returns empty list |
| 3.6 | `GetErrors` — single failed chunk | Returns list with 1 error message |
| 3.7 | `GetErrors` — multiple failed chunks | All error messages collected |
| 3.8 | `GetErrors` — skips completed chunks | Only failed chunk errors returned |
| 3.9 | `GetErrors` — no failed chunks | Returns empty list |
| 3.10 | `GetFailedChunkInputs` — returns original input payload of failed chunks | Input JSON matches what was stored |
| 3.11 | `GetFailedChunkInputs` — skips completed chunks | Only failed inputs returned |
| 3.12 | `GetFailedChunkInputs` — no failed chunks | Returns empty list |

---

## 4. PW Batch Coordinator — Cleanup

| # | Scenario | What we verify |
|---|---|---|
| 4.1 | `Cleanup` deletes batch record | `Batch.Get()` returns false after cleanup |
| 4.2 | `Cleanup` deletes all chunk records for that batch | No chunks remain with that BatchId |
| 4.3 | `Cleanup` does not affect other batches | Other batch's records untouched |

---

## 5. PW Batch Cleanup (scheduled cleanup)

| # | Scenario | What we verify |
|---|---|---|
| 5.1 | Deletes `Completed` batch older than threshold | Batch and its chunks are gone |
| 5.2 | Deletes `Failed` batch older than threshold | Batch and its chunks are gone |
| 5.3 | Deletes `PartialFailure` batch older than threshold | Batch and its chunks are gone |
| 5.4 | Does **not** delete `Running` batch | Batch remains |
| 5.5 | Does **not** delete `Pending` batch | Batch remains |
| 5.6 | Does **not** delete recent `Completed` batch (within threshold) | Batch remains |
| 5.7 | Mixed scenario — old completed + running + recent completed | Only old completed deleted |

---

## 6. Integration Tests — RunForList End-to-End

Uses a test worker (`PW Test Worker`) registered via enum extension. Calls `RunForList`, waits, verifies results. Cleanup after each test.

| # | Scenario | What we verify |
|---|---|---|
| 6.1 | `RunForList` with single thread, single item | Batch completes, 1 result returned |
| 6.2 | `RunForList` with multiple threads, multiple items | All items processed, results from all chunks collected |
| 6.3 | `RunForList` splits items evenly across threads | Chunk count matches expected split |
| 6.4 | `RunForList` with fewer items than threads | Chunk count = item count (no empty chunks) |
| 6.5 | `RunForList` with empty list | Returns null GUID, no batch created |
| 6.6 | `WaitForCompletion` returns true on success | All chunks completed |
| 6.7 | `GetCompletedChunks` / `GetTotalChunks` correct after completion | Counters match actual chunk count |

---

## 7. Integration Tests — RunForChunks End-to-End

| # | Scenario | What we verify |
|---|---|---|
| 7.1 | `RunForChunks` with pre-built payloads | Each chunk receives its exact input, results collected |
| 7.2 | `RunForChunks` ignores `SetThreads` | Chunk count = number of JsonObjects passed, not thread count |
| 7.3 | `RunForChunks` with empty list | Returns null GUID, no batch created |

---

## 8. Integration Tests — RunForRecords End-to-End

Uses a test worker that reads records via `GetRecordRef` and counts them.

| # | Scenario | What we verify |
|---|---|---|
| 8.1 | `RunForRecords` splits filtered records across threads | Each chunk processes its slice, total count matches |
| 8.2 | `GetRecordRef` positions cursor correctly | Worker reads correct records per chunk |
| 8.3 | `GetChunkSize` matches actual record count per chunk | Worker can iterate exactly `GetChunkSize` records |
| 8.4 | `RunForRecords` with fewer records than threads | Chunk count = record count |
| 8.5 | `RunForRecords` with zero records | Returns null GUID, no batch created |

---

## 9. Integration Tests — Error Handling

| # | Scenario | What we verify |
|---|---|---|
| 9.1 | Worker that raises `Error()` | Chunk marked `Failed`, error message captured |
| 9.2 | Partial failure — some chunks succeed, some fail | Batch status = `PartialFailure`, `GetResults` returns partial results, `GetErrors` returns error messages |
| 9.3 | All chunks fail | Batch status = `Failed` |
| 9.4 | `GetFailedChunkInputs` returns original inputs for retry | Can re-submit failed inputs via `RunForChunks` |
| 9.5 | Worker that calls `Commit()` | `CommitBehavior::Error` triggers, chunk marked `Failed` with runtime error |
| 9.6 | Worker database writes rolled back on failure | Worker modifies a record then errors — record unchanged after batch completes |

---

## 10. Integration Tests — Timeout & Configuration

| # | Scenario | What we verify |
|---|---|---|
| 10.1 | `SetTimeout` causes `WaitForCompletion` to return false on slow batch | Returns false before batch finishes |
| 10.2 | `SetPollInterval` is respected | Polling uses configured interval |
| 10.3 | Fluent builder chaining | `SetThreads().SetTimeout().SetPollInterval()` returns same codeunit instance |

---

## 11. Test Infrastructure Needed

| Object | ID | Purpose |
|---|---|---|
| `PW Test Helper` (Codeunit) | 99204 | Helper procedures: `CreateBatch`, `CreateChunkWithPayload`, `CreateChunkWithResult`, `CreateChunkWithError` |
| `PW Test Worker Type` (EnumExt) | 99201 | Extends `PW Worker Type` with test values (success worker, error worker, commit worker) |
| `PW Test Worker` (Codeunit) | 99205 | Simple worker: reads `$Items`, returns count. For success-path integration tests |
| `PW Test Error Worker` (Codeunit) | 99206 | Worker that raises `Error()`. For failure-path integration tests |
| `PW Test Commit Worker` (Codeunit) | 99207 | Worker that calls `Commit()`. For CommitBehavior enforcement test |
| `PW Test Record Worker` (Codeunit) | 99208 | Worker that reads via `GetRecordRef`, counts records. For RunForRecords tests |
| Update `test/app.json` | — | Add dependency on main Parallel Worker app |
