# ParallelWorker Library — Implementation Plan

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  Consumer API                                           │
│  PW Batch Coordinator  (fluent builder, Run*, results)  │
├─────────────────────────────────────────────────────────┤
│  Execution Engine                                       │
│  PW Task Dispatcher  (StartSession entry, error capture)│
│  PW Chunk Context    (input/output for workers)         │
├─────────────────────────────────────────────────────────┤
│  State Store                                            │
│  PW Batch  /  PW Batch Chunk  (DB as IPC channel)       │
└─────────────────────────────────────────────────────────┘
```

## Key Design Decisions (vs. conversation)

| Decision | Conversation said | This plan | Why |
|---|---|---|---|
| Worker dispatch | CodeunitId (integer) | Enum implementing interface | Type-safe, AL-idiomatic, extensible via `enumextension` |
| Interfaces | Two (`IParallelWorker` + `IParallelRecordWorker`) | One (`IParallelWorker`) | Simpler — `ChunkContext.GetRecordRef()` handles record workers without a second interface |
| `ConfigureClient` on interface | Added for HTTP workers | Dropped | HTTP-specific concern doesn't belong on a core interface. Workers configure their own clients in `Execute` |
| Error handling | Not detailed | `[TryFunction]` wrapping interface call | Centralized in dispatcher — worker never worries about error capture |
| Result storage | Worker calls `Save()` | Dispatcher reads from `ChunkContext` after `Execute` returns | Worker can't forget to save; dispatcher owns persistence |
| `Await` / `TaskFuture` | Dropped then re-added as `WaitForCompletion` | Both blocking (`WaitForCompletion`) and non-blocking (`GetStatus`) | Blocking is fine for short batches; non-blocking for long ones. Document the tradeoff |
| Cancellation / retry / priorities | Discussed then dropped | Not included | Honest about AL constraints. Can be added later if needed |

## Object Map

All objects use the `PW` prefix and ID range `99000-99200`.

### Enums

| ID | Name | Purpose |
|---|---|---|
| 99000 | `PW Batch Status` | `Pending`, `Running`, `Completed`, `PartialFailure`, `Failed` |
| 99001 | `PW Chunk Status` | `Pending`, `Running`, `Completed`, `Failed` |
| 99002 | `PW Worker Type` | Extensible enum implementing `PW IParallel Worker`. Ships empty — consumers extend it |

### Interface

| Name | Purpose |
|---|---|
| `PW IParallel Worker` | Single method: `Execute(var ChunkContext: Codeunit "PW Chunk Context")` |

### Tables

| ID | Name | Purpose |
|---|---|---|
| 99000 | `PW Batch` | Batch header — status, worker type, chunk counts, timestamps. Small rows, frequently polled |
| 99001 | `PW Batch Chunk` | Per-chunk state — input/result BLOBs, error info, session ID. Written once per chunk |

### Codeunits

| ID | Name | Purpose |
|---|---|---|
| 99000 | `PW Batch Coordinator` | Consumer-facing API. Fluent builder (`SetThreads`, `SetTimeout`). Three `Run*` methods. Result collection. Cleanup |
| 99001 | `PW Chunk Context` | Passed to worker's `Execute`. Provides typed input accessors (`GetIntInput`, `GetTextInput`, etc.), `SetResult`/`AppendResult` for output, `GetRecordRef` for record-based chunks |
| 99002 | `PW Task Dispatcher` | `TableNo = "PW Batch Chunk"`. Entry point for `StartSession`. Claims chunk, calls worker via interface, captures errors, updates counters |
| 99003 | `PW Batch Cleanup` | Deletes batches older than N hours. Can be scheduled via Job Queue |

### Pages (monitoring)

| ID | Name | Purpose |
|---|---|---|
| 99000 | `PW Batches` | List page — batch status, progress, timestamps |
| 99001 | `PW Batch Chunk List` | List/subpage — chunk details, errors, session IDs |

**Total: 14 objects** (3 enums, 1 interface, 2 tables, 4 codeunits, 2 pages)

---

## Detailed Design

### 1. Enums

**`PW Batch Status` (99000)**

```
Pending(0) -> Running(1) -> Completed(2) | PartialFailure(3) | Failed(4)
```

- `Pending`: batch record created, sessions not yet started
- `PartialFailure`: some chunks succeeded, some failed

**`PW Chunk Status` (99001)**

```
Pending(0) -> Running(1) -> Completed(2) | Failed(3)
```

**`PW Worker Type` (99002)**

```al
enum 99002 "PW Worker Type" implements "PW IParallel Worker"
{
    Extensible = true;
    // Ships empty — consumers add values via enumextension
}
```

### 2. Interface — `PW IParallel Worker`

```al
interface "PW IParallel Worker"
{
    procedure Execute(var ChunkContext: Codeunit "PW Chunk Context");
}
```

One method. No `ConfigureClient`, no setup hooks. Workers handle their own initialization inside `Execute` using data from `ChunkContext.GetInput()`.

### 3. Table — `PW Batch` (99000)

| # | Field | Type | Notes |
|---|---|---|---|
| 1 | `Id` | Guid | PK. Auto-generated |
| 2 | `Status` | Enum "PW Batch Status" | |
| 3 | `Worker Type` | Enum "PW Worker Type" | Which worker to dispatch |
| 4 | `Total Chunks` | Integer | Set at creation |
| 5 | `Completed Chunks` | Integer | Incremented by dispatcher |
| 6 | `Failed Chunks` | Integer | Incremented by dispatcher |
| 7 | `Timeout Seconds` | Integer | 0 = no timeout |
| 8 | `Created At` | DateTime | |
| 9 | `Completed At` | DateTime | Set when all chunks finish |
| 10 | `Company Name` | Text[30] | Company context for sessions |

**Key design**: No BLOBs on this table. It stays small for fast polling by `WaitForCompletion` and `GetStatus`. All payload data lives on `PW Batch Chunk`.

### 4. Table — `PW Batch Chunk` (99001)

| # | Field | Type | Notes |
|---|---|---|---|
| 1 | `Batch Id` | Guid | PK part 1, links to PW Batch |
| 2 | `Chunk Index` | Integer | PK part 2 (1-based) |
| 3 | `Status` | Enum "PW Chunk Status" | |
| 4 | `Input Payload` | Blob | JsonObject serialized |
| 5 | `Result Payload` | Blob | JsonArray serialized (supports AppendResult) |
| 6 | `Error Message` | Text[2048] | From GetLastErrorText() |
| 7 | `Error Call Stack` | Blob | From GetLastErrorCallStack() |
| 8 | `Session Id` | Integer | For crash detection / debugging |
| 9 | `Started At` | DateTime | |
| 10 | `Completed At` | DateTime | |

**Composite PK**: (`Batch Id`, `Chunk Index`). No secondary keys needed initially — the dispatcher reads by PK, the coordinator filters by `Batch Id`.

### 5. Codeunit — `PW Chunk Context` (99001)

This is the worker's view of its chunk. Initialized by the dispatcher before calling `Execute`.

**Internal state:**

```
InputJson: JsonObject     <- deserialized from Input Payload blob
Results: JsonArray        <- accumulated via SetResult/AppendResult
BatchId: Guid
ChunkIndex: Integer
```

**Methods:**

```al
// --- Lifecycle (called by dispatcher, not by worker) ---
procedure Init(var Chunk: Record "PW Batch Chunk")
// Reads Input Payload blob -> InputJson. Stores BatchId, ChunkIndex.

procedure SaveResultsTo(var Chunk: Record "PW Batch Chunk")
// Writes Results JsonArray -> Result Payload blob.

// --- Input accessors (called by worker) ---
procedure GetBatchId(): Guid
procedure GetChunkIndex(): Integer
procedure GetInput(): JsonObject              // raw access
procedure GetTextInput(Key: Text): Text       // convenience, errors on missing key
procedure GetIntInput(Key: Text): Integer
procedure GetDecimalInput(Key: Text): Decimal
procedure GetBoolInput(Key: Text): Boolean
procedure GetInputArray(Key: Text): JsonArray

// --- RecordRef helper (for RunForRecords chunks) ---
procedure IsRecordChunk(): Boolean
// Returns true if input contains system keys ($TableNo, $FilterView, etc.)

procedure GetRecordRef(var RecRef: RecordRef)
// Opens table, applies filter view, calls FindSet + Next(StartIndex-1).
// Worker receives a positioned RecordRef ready to iterate.

procedure GetEndIndex(): Integer
// How many records this chunk should process.

// --- Output (called by worker) ---
procedure SetResult(Result: JsonObject)
// Single result — clears previous, adds to Results array.

procedure AppendResult(Result: JsonObject)
// Multi-row result — appends to Results array.
```

**System keys convention**: Keys prefixed with `$` are reserved for the coordinator (`$TableNo`, `$FilterView`, `$StartIndex`, `$EndIndex`, `$Items`, `$IsRecordChunk`). User payload keys must not start with `$`.

### 6. Codeunit — `PW Task Dispatcher` (99002)

`TableNo = "PW Batch Chunk"`. This is what `StartSession` calls.

**OnRun flow:**

```
1. Read batch record -> get Worker Type
2. Claim chunk: set Status=Running, SessionId=SessionId(), StartedAt=Now
3. COMMIT  <- critical: persists Running state before worker executes
4. Init ChunkContext from chunk record
5. TryExecuteWorker(WorkerType, ChunkContext)
   |-- Success:
   |   Context.SaveResultsTo(Chunk)
   |   Chunk.Status := Completed
   |   Chunk.Modify()
   +-- Failure:
       Chunk.Get(...)  <- re-read (TryFunction rolled back worker's changes)
       Chunk."Error Message" := GetLastErrorText()
       Write GetLastErrorCallStack() to Error Call Stack blob
       Chunk.Status := Failed
       Chunk.Modify()
6. UpdateBatchCounters(BatchId)
```

**`[TryFunction]` wrapper:**

```al
[TryFunction]
local procedure TryExecuteWorker(WorkerType: Enum "PW Worker Type"; var Context: Codeunit "PW Chunk Context")
var
    Worker: Interface "PW IParallel Worker";
begin
    Worker := WorkerType;
    Worker.Execute(Context);
end;
```

**UpdateBatchCounters** must handle concurrent updates (multiple chunks finishing simultaneously). Use `LockTable` on the batch record before reading counts to prevent race conditions:

```al
Batch.LockTable();
Batch.Get(BatchId);
// count chunks by status...
Batch.Modify();
Commit();
```

### 7. Codeunit — `PW Batch Coordinator` (99000)

Consumer-facing API with fluent builder using `this` keyword.

**Internal state:**

```
ThreadCount: Integer      (default: 4)
TimeoutSeconds: Integer   (default: 0 = none)
PollIntervalMs: Integer   (default: 500)
```

**Fluent configuration:**

```al
procedure SetThreads(Count: Integer): Codeunit "PW Batch Coordinator"
begin
    ThreadCount := Count;
    exit(this);
end;

procedure SetTimeout(Seconds: Integer): Codeunit "PW Batch Coordinator"
procedure SetPollInterval(Milliseconds: Integer): Codeunit "PW Batch Coordinator"
```

**Three Run methods** — all return `BatchId: Guid`:

**`RunForRecords(WorkerType, var RecRef: RecordRef, BasePayload: JsonObject): Guid`**

1. `RecRef.Count()` -> total records
2. Calculate chunk size = `Ceiling(total / ThreadCount)`
3. Create `PW Batch` record (status=Pending, worker type, total chunks)
4. For each chunk (up to ThreadCount):
   - Clone `BasePayload`
   - Add system keys: `$IsRecordChunk=true`, `$TableNo`, `$FilterView` (from `RecRef.GetView()`), `$StartIndex`, `$EndIndex`
   - Create `PW Batch Chunk` record, write payload to Input Payload blob
5. If total records < ThreadCount -> fewer chunks than threads (don't create empty chunks)
6. Update batch `Total Chunks` to actual count
7. `Commit()` <- **must commit before StartSession** (otherwise rollback loses records but sessions run)
8. Set batch status = Running, `Modify()`, `Commit()`
9. For each chunk: `StartSession(SessionId, Codeunit::"PW Task Dispatcher", CompanyName, ChunkRecord)`
10. Return BatchId

**`RunForList(WorkerType, Items: List of [Text], BasePayload: JsonObject): Guid`**

1. Split `Items` into `ThreadCount` sublists
2. Each chunk's payload: clone BasePayload + add `$Items` as JsonArray
3. Same create/commit/start pattern as RunForRecords

**`RunForChunks(WorkerType, Chunks: List of [JsonObject]): Guid`**

1. One chunk per JsonObject in the list — caller controls the split
2. ThreadCount is ignored (chunk count = session count)
3. Same create/commit/start pattern

**Completion and results:**

```al
// Blocking — polls DB until all chunks done or timeout
procedure WaitForCompletion(BatchId: Guid): Boolean
// Returns true if Completed, false if Failed/PartialFailure/TimedOut.
// Uses Sleep(PollIntervalMs) between polls.
// Documents: "This holds the calling session open."

// Non-blocking status check
procedure GetStatus(BatchId: Guid): Enum "PW Batch Status"
procedure GetCompletedChunks(BatchId: Guid): Integer
procedure GetTotalChunks(BatchId: Guid): Integer
procedure IsFinished(BatchId: Guid): Boolean   // completed, failed, or partial

// Results — call after completion
procedure GetResults(BatchId: Guid; var Results: List of [JsonObject])
// Reads all completed chunks' Result Payload blobs.
// Flattens all JsonArrays into one list.

procedure GetErrors(BatchId: Guid; var Errors: List of [Text])
// Reads all failed chunks' Error Message fields.

// Recovery — for retrying failed chunks
procedure GetFailedChunkInputs(BatchId: Guid; var FailedInputs: List of [JsonObject])
// Reads Input Payload from chunks where Status = Failed.
// Returns the original inputs so consumer can pass them to a new RunForChunks call.

// Cleanup
procedure Cleanup(BatchId: Guid)
// Deletes batch + all chunk records for this batch.
```

### 8. Codeunit — `PW Batch Cleanup` (99003)

Simple codeunit with `OnRun` so it can be scheduled via Job Queue.

```al
procedure CleanupOlderThan(Hours: Integer)
// Deletes PW Batch + PW Batch Chunk records where
// CompletedAt < CurrentDateTime - Hours
// AND Status IN (Completed, Failed, PartialFailure)
// Never deletes Running/Pending batches.
```

### 9. Pages

**`PW Batches` (99000)** — List page on `PW Batch`

- Columns: Id, Status, Worker Type, Progress (Completed/Total), Created At, Completed At
- Actions: View Chunks (drilldown), Cleanup Selected, Cleanup All Older Than...

**`PW Batch Chunk List` (99001)** — List page on `PW Batch Chunk`

- Columns: Chunk Index, Status, Session Id, Started At, Completed At, Error Message
- Actions: View Error Call Stack, View Input Payload, View Result Payload

---

## Implementation Phases

### Phase 1 — Foundation (enums, interface, tables)

Create all enums, the interface, and both tables. This is pure structure — no logic. Verify it compiles and syncs.

**Files:**

- `src/Enums/PWBatchStatus.Enum.al`
- `src/Enums/PWChunkStatus.Enum.al`
- `src/Enums/PWWorkerType.Enum.al`
- `src/Interfaces/PWIParallelWorker.Interface.al`
- `src/Tables/PWBatch.Table.al`
- `src/Tables/PWBatchChunk.Table.al`

### Phase 2 — Core Engine (context + dispatcher)

Build `PW Chunk Context` (all input/output methods) and `PW Task Dispatcher` (session entry point, error handling, counter updates). Test with a trivial worker that reads input and writes a result.

**Files:**

- `src/Codeunits/PWChunkContext.Codeunit.al`
- `src/Codeunits/PWTaskDispatcher.Codeunit.al`

### Phase 3 — Consumer API (coordinator)

Build `PW Batch Coordinator` with fluent API. Implement `RunForChunks` first (simplest — caller defines chunks). Then `RunForRecords` (adds splitting logic). Then `RunForList`. Add `WaitForCompletion`, `GetStatus`, `GetResults`, `Cleanup`.

**Files:**

- `src/Codeunits/PWBatchCoordinator.Codeunit.al`

### Phase 4 — Monitoring and Cleanup

Build the list pages and cleanup codeunit. These are quality-of-life additions.

**Files:**

- `src/Pages/PWBatches.Page.al`
- `src/Pages/PWBatchChunkList.Page.al`
- `src/Codeunits/PWBatchCleanup.Codeunit.al`

### Phase 5 — Sample Worker and Testing

Create a sample worker (e.g., parallel record counting by filter) as both a test and a usage example. Create an enumextension and the worker codeunit in a `test/` folder.

---

## Consumer Usage Examples

### 1. Parallel calculation (fire-and-forget with status check)

```al
// Define chunks by posting group
foreach PostingGroup in PostingGroups do begin
    Chunk.Add('PostingGroupCode', PostingGroup.Code);
    Chunks.Add(Chunk);
    Clear(Chunk);
end;

BatchId := Coordinator
    .SetTimeout(3600)
    .RunForChunks("PW Worker Type"::RecalcCost, Chunks);

// Store BatchId, check later via Coordinator.GetStatus(BatchId)
```

### 2. Parallel record processing (blocking)

```al
Item.SetRange("Item Category Code", 'FURNITURE');
RecRef.GetTable(Item);

BatchId := Coordinator
    .SetThreads(6)
    .SetTimeout(300)
    .RunForRecords("PW Worker Type"::ValidateItems, RecRef, EmptyPayload);

if Coordinator.WaitForCompletion(BatchId) then
    Message('All items validated.')
else begin
    Coordinator.GetErrors(BatchId, Errors);
    // handle errors
end;
Coordinator.Cleanup(BatchId);
```

### 3. Consumer's worker implementation

```al
// 1. Codeunit implementing the interface
codeunit 50100 "My Validate Items Worker" implements "PW IParallel Worker"
{
    procedure Execute(var Ctx: Codeunit "PW Chunk Context")
    var
        RecRef: RecordRef;
        EndIdx: Integer;
        Processed: Integer;
    begin
        Ctx.GetRecordRef(RecRef);
        EndIdx := Ctx.GetEndIndex();
        repeat
            ValidateItem(RecRef);
            Processed += 1;
        until (RecRef.Next() = 0) or (Processed >= EndIdx);
    end;
}

// 2. Enum extension (5 lines of ceremony)
enumextension 50000 "My Workers" extends "PW Worker Type"
{
    value(50000; ValidateItems)
    {
        Implementation = "PW IParallel Worker" = "My Validate Items Worker";
    }
}
```

---

## Write Safety

Workers are allowed to write to the database. This is often the whole point — parallel recalculation, parallel data transformation, parallel posting preparation. But parallel writes carry real risks that the library cannot prevent at the AL level. The library's approach is: **allow writes, document the rules, surface failures clearly, provide recovery tools**.

### Safe vs. dangerous write patterns

| Pattern | Safety | Why |
|---|---|---|
| Read-only workers (export, aggregation, validation) | Safe | No write conflicts possible |
| Write to completely independent tables per chunk | Safe | No shared lock surface |
| Write to same table, non-overlapping record ranges | Risky | SQL page-level locks can still collide |
| Write to same table, touching shared records | Dangerous | Deadlocks, lost updates |
| Calling `Commit()` inside worker | Dangerous | Partial state on failure — `TryFunction` can only roll back uncommitted changes |
| Updating FlowFields / running totals | Dangerous | Inherently serial operations |
| Inserting with auto-increment `Entry No.` | Dangerous | Number sequence contention |

### What happens on partial failure

If a batch has 6 chunks and chunks 1-4 succeed but 5-6 fail, the batch status is `PartialFailure`. Chunks that succeeded have already committed their writes — there is no distributed rollback across sessions. The database is in a half-updated state.

This is **by design**. The library makes this state visible rather than hiding it:

- `PartialFailure` status forces the consumer to acknowledge the situation
- `GetErrors()` shows what went wrong in each failed chunk
- `GetFailedChunkInputs()` returns the original inputs of failed chunks so they can be retried

### Design rules for workers that write

1. **Operate on non-overlapping data.** Each chunk must own its slice exclusively. If using `RunForRecords`, the coordinator guarantees non-overlapping index ranges — but beware of secondary tables touched by business logic (e.g., recalculating an Item might write to Item Ledger Entry, which other chunks also touch).

2. **Design for idempotency.** A worker should produce the same result whether it runs once or twice on the same input. This makes retry after `PartialFailure` safe. Practical pattern: check if the work is already done before doing it, or use `ModifyAll` / upsert patterns rather than conditional insert-or-update.

3. **Avoid `Commit()` inside workers.** Let the dispatcher own the transaction boundary. If the worker errors after a mid-execution `Commit()`, the committed writes are permanent but the chunk is marked `Failed` — creating invisible partial state that no one can track.

4. **Keep chunk size large enough to amortize session cost, small enough to limit blast radius.** If a chunk fails, all its work is rolled back (assuming no internal `Commit()`). Smaller chunks mean less wasted work on failure, but more session overhead.

### Recovery: `GetFailedChunkInputs()`

Added to `PW Batch Coordinator` to support retry after partial failure:

```al
procedure GetFailedChunkInputs(BatchId: Guid; var FailedInputs: List of [JsonObject])
// Reads Input Payload from all chunks where Status = Failed.
// Consumer can pass these to a new RunForChunks call to retry just the failed work.
```

**Retry pattern:**

```al
BatchId := Coordinator
    .SetThreads(6)
    .RunForRecords("PW Worker Type"::RecalcCost, RecRef, Payload);

if not Coordinator.WaitForCompletion(BatchId) then
    if Coordinator.GetStatus(BatchId) = "PW Batch Status"::PartialFailure then begin
        // Retry only the failed chunks
        Coordinator.GetFailedChunkInputs(BatchId, FailedInputs);
        RetryBatchId := Coordinator
            .SetThreads(FailedInputs.Count())
            .RunForChunks("PW Worker Type"::RecalcCost, FailedInputs);

        if Coordinator.WaitForCompletion(RetryBatchId) then
            Message('All chunks completed after retry.')
        else
            Error('Retry failed. Check PW Batches page for details.');

        Coordinator.Cleanup(RetryBatchId);
    end;

Coordinator.Cleanup(BatchId);
```

This pattern only works correctly when workers are idempotent — retrying a chunk that partially committed (due to internal `Commit()`) could produce duplicates or wrong totals.

---

## Known Constraints (document clearly)

1. **`Commit()` before dispatch** — `RunFor*` methods commit the current transaction. Document this prominently.
2. **Session limits** — each chunk = one session. Don't create 50 chunks on a tenant with 10 session slots.
3. **No cancellation** — once started, chunks run to completion or failure. No cooperative cancellation.
4. **`WaitForCompletion` holds a session** — use non-blocking `GetStatus` for long-running batches.
5. **JSON serialization** — no compile-time type safety on payloads. Use `ChunkContext.GetTextInput()` etc. for fail-fast errors.
6. **No restart survival** — uses `StartSession`, not Job Queue. Server restart kills running chunks (they stay as "Running" forever unless cleaned up).
7. **No distributed transactions** — each chunk session owns its own transaction. Partial failure means partial writes. Design workers to be idempotent.
