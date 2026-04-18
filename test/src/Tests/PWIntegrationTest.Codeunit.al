codeunit 99209 "PW Integration Test"
{
    Subtype = Test;
    TestPermissions = Disabled;

    var
        LibraryAssert: Codeunit "Library Assert";

    #region RunForList End-to-End

    [Test]
    procedure RunForListSingleItem()
    // [SCENARIO 6.1] RunForList with single thread and single item
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        Items: List of [Text];
        Payload: JsonObject;
        Results: List of [JsonObject];
        BatchId: Guid;
    begin
        Items.Add('Item-001');

        BatchId := Coordinator
            .SetThreads(1)
            .SetBatchTimeout(30)
            .RunForList("PW Worker Type"::TestWorker, Items, Payload);

        LibraryAssert.IsTrue(Coordinator.WaitForCompletion(BatchId), 'Batch should complete');
        Coordinator.GetResults(BatchId, Results);
        LibraryAssert.AreEqual(1, Results.Count(), 'Should have 1 result');

        Coordinator.Cleanup(BatchId);
        Commit();
    end;

    [Test]
    procedure RunForListMultipleItems()
    // [SCENARIO 6.2] RunForList with multiple threads — all items processed
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        Items: List of [Text];
        Payload: JsonObject;
        Results: List of [JsonObject];
        BatchId: Guid;
        ResultObj: JsonObject;
        Token: JsonToken;
        TotalItems: Integer;
        i: Integer;
    begin
        for i := 1 to 10 do
            Items.Add(StrSubstNo('Item-%1', i));

        BatchId := Coordinator
            .SetThreads(3)
            .SetBatchTimeout(30)
            .RunForList("PW Worker Type"::TestWorker, Items, Payload);

        LibraryAssert.IsTrue(Coordinator.WaitForCompletion(BatchId), 'Batch should complete');
        Coordinator.GetResults(BatchId, Results);

        TotalItems := 0;
        for i := 1 to Results.Count() do begin
            ResultObj := Results.Get(i);
            ResultObj.Get('ItemCount', Token);
            TotalItems += Token.AsValue().AsInteger();
        end;
        LibraryAssert.AreEqual(10, TotalItems, 'Total items across chunks should be 10');

        Coordinator.Cleanup(BatchId);
        Commit();
    end;

    [Test]
    procedure RunForListSplitsEvenly()
    // [SCENARIO 6.3] RunForList splits items across threads
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        Items: List of [Text];
        Payload: JsonObject;
        BatchId: Guid;
        i: Integer;
    begin
        for i := 1 to 9 do
            Items.Add(StrSubstNo('Item-%1', i));

        BatchId := Coordinator
            .SetThreads(3)
            .SetBatchTimeout(30)
            .RunForList("PW Worker Type"::TestWorker, Items, Payload);

        Coordinator.WaitForCompletion(BatchId);
        LibraryAssert.AreEqual(3, Coordinator.GetTotalChunks(BatchId), 'Should have 3 chunks');

        Coordinator.Cleanup(BatchId);
        Commit();
    end;

    [Test]
    procedure RunForListFewerItemsThanThreads()
    // [SCENARIO 6.4] RunForList with fewer items than threads — chunk count = item count
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        Items: List of [Text];
        Payload: JsonObject;
        BatchId: Guid;
    begin
        Items.Add('A');
        Items.Add('B');

        BatchId := Coordinator
            .SetThreads(5)
            .SetBatchTimeout(30)
            .RunForList("PW Worker Type"::TestWorker, Items, Payload);

        Coordinator.WaitForCompletion(BatchId);
        LibraryAssert.AreEqual(2, Coordinator.GetTotalChunks(BatchId), 'Chunk count should equal item count');

        Coordinator.Cleanup(BatchId);
        Commit();
    end;

    [Test]
    procedure RunForListEmptyList()
    // [SCENARIO 6.5] RunForList with empty list returns null GUID
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        Items: List of [Text];
        Payload: JsonObject;
        BatchId: Guid;
    begin
        BatchId := Coordinator.RunForList("PW Worker Type"::TestWorker, Items, Payload);

        LibraryAssert.IsTrue(IsNullGuid(BatchId), 'Empty list should return null GUID');
    end;

    [Test]
    procedure RunForListWaitReturnsTrue()
    // [SCENARIO 6.6] WaitForCompletion returns true on successful batch
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        Items: List of [Text];
        Payload: JsonObject;
        BatchId: Guid;
    begin
        Items.Add('X');

        BatchId := Coordinator
            .SetThreads(1)
            .SetBatchTimeout(30)
            .RunForList("PW Worker Type"::TestWorker, Items, Payload);

        LibraryAssert.IsTrue(Coordinator.WaitForCompletion(BatchId), 'WaitForCompletion should return true');
        LibraryAssert.IsTrue(
            Coordinator.GetStatus(BatchId) = "PW Batch Status"::Completed,
            'Status should be Completed');

        Coordinator.Cleanup(BatchId);
        Commit();
    end;

    [Test]
    procedure RunForListCountersCorrectAfterCompletion()
    // [SCENARIO 6.7] Completed and total chunk counters match after completion
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        Items: List of [Text];
        Payload: JsonObject;
        BatchId: Guid;
        i: Integer;
    begin
        for i := 1 to 4 do
            Items.Add(StrSubstNo('Item-%1', i));

        BatchId := Coordinator
            .SetThreads(2)
            .SetBatchTimeout(30)
            .RunForList("PW Worker Type"::TestWorker, Items, Payload);

        Coordinator.WaitForCompletion(BatchId);
        LibraryAssert.AreEqual(
            Coordinator.GetTotalChunks(BatchId),
            Coordinator.GetCompletedChunks(BatchId),
            'Completed chunks should equal total chunks');

        Coordinator.Cleanup(BatchId);
        Commit();
    end;

    #endregion

    #region RunForChunks End-to-End

    [Test]
    procedure RunForChunksWithPayloads()
    // [SCENARIO 7.1] RunForChunks with pre-built payloads
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        Chunks: List of [JsonObject];
        Chunk: JsonObject;
        Items: JsonArray;
        Results: List of [JsonObject];
        BatchId: Guid;
    begin
        Items.Add('A');
        Items.Add('B');
        Chunk.Add('$Items', Items);
        Chunks.Add(Chunk);

        Clear(Chunk);
        Clear(Items);
        Items.Add('C');
        Chunk.Add('$Items', Items);
        Chunks.Add(Chunk);

        BatchId := Coordinator
            .SetBatchTimeout(30)
            .RunForChunks("PW Worker Type"::TestWorker, Chunks);

        LibraryAssert.IsTrue(Coordinator.WaitForCompletion(BatchId), 'Batch should complete');
        Coordinator.GetResults(BatchId, Results);
        LibraryAssert.AreEqual(2, Results.Count(), 'Should have 2 results');

        Coordinator.Cleanup(BatchId);
        Commit();
    end;

    [Test]
    procedure RunForChunksIgnoresThreadCount()
    // [SCENARIO 7.2] RunForChunks — chunk count equals number of JsonObjects, not SetThreads
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        Chunks: List of [JsonObject];
        Chunk: JsonObject;
        BatchId: Guid;
        i: Integer;
    begin
        for i := 1 to 3 do begin
            Clear(Chunk);
            Chunk.Add('Index', i);
            Chunks.Add(Chunk);
        end;

        BatchId := Coordinator
            .SetThreads(10)
            .SetBatchTimeout(30)
            .RunForChunks("PW Worker Type"::TestWorker, Chunks);

        Coordinator.WaitForCompletion(BatchId);
        LibraryAssert.AreEqual(3, Coordinator.GetTotalChunks(BatchId),
            'Should have 3 chunks regardless of thread count');

        Coordinator.Cleanup(BatchId);
        Commit();
    end;

    [Test]
    procedure RunForChunksEmptyList()
    // [SCENARIO 7.3] RunForChunks with empty list returns null GUID
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        Chunks: List of [JsonObject];
        BatchId: Guid;
    begin
        BatchId := Coordinator.RunForChunks("PW Worker Type"::TestWorker, Chunks);

        LibraryAssert.IsTrue(IsNullGuid(BatchId), 'Empty chunks should return null GUID');
    end;

    #endregion

    #region RunForRecords End-to-End

    [Test]
    procedure RunForRecordsSplitsAcrossThreads()
    // [SCENARIO 8.1] RunForRecords splits records and total count matches
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        TestBatch: Record "PW Batch";
        RecRef: RecordRef;
        Payload: JsonObject;
        Results: List of [JsonObject];
        BatchId: Guid;
        ResultObj: JsonObject;
        Token: JsonToken;
        TotalRecords: Integer;
        ActualTotal: Integer;
        i: Integer;
    begin
        // Pre-cleanup from previous failed runs
        TestBatch.SetRange("Company Name", 'PW_TEST_DATA');
        TestBatch.DeleteAll();

        for i := 1 to 6 do begin
            Clear(TestBatch);
            TestBatch.Id := CreateGuid();
            TestBatch.Status := "PW Batch Status"::Pending;
            TestBatch."Company Name" := 'PW_TEST_DATA';
            TestBatch.Insert();
        end;
        Commit();

        TestBatch.Reset();
        TestBatch.SetRange("Company Name", 'PW_TEST_DATA');
        RecRef.GetTable(TestBatch);
        TotalRecords := RecRef.Count();

        BatchId := Coordinator
            .SetThreads(2)
            .SetBatchTimeout(30)
            .RunForRecords("PW Worker Type"::TestRecordWorker, RecRef, Payload);

        LibraryAssert.IsTrue(Coordinator.WaitForCompletion(BatchId), 'Batch should complete');
        Coordinator.GetResults(BatchId, Results);

        ActualTotal := 0;
        for i := 1 to Results.Count() do begin
            ResultObj := Results.Get(i);
            ResultObj.Get('RecordCount', Token);
            ActualTotal += Token.AsValue().AsInteger();
        end;
        LibraryAssert.AreEqual(TotalRecords, ActualTotal, 'Total records processed should match');

        Coordinator.Cleanup(BatchId);
        TestBatch.Reset();
        TestBatch.SetRange("Company Name", 'PW_TEST_DATA');
        TestBatch.DeleteAll();
        Commit();
    end;

    [Test]
    procedure RunForRecordsFewerRecordsThanThreads()
    // [SCENARIO 8.4] RunForRecords with fewer records than threads — no empty chunks
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        TestBatch: Record "PW Batch";
        RecRef: RecordRef;
        Payload: JsonObject;
        BatchId: Guid;
        i: Integer;
    begin
        TestBatch.SetRange("Company Name", 'PW_TEST_FEW');
        TestBatch.DeleteAll();

        for i := 1 to 2 do begin
            Clear(TestBatch);
            TestBatch.Id := CreateGuid();
            TestBatch.Status := "PW Batch Status"::Pending;
            TestBatch."Company Name" := 'PW_TEST_FEW';
            TestBatch.Insert();
        end;
        Commit();

        TestBatch.Reset();
        TestBatch.SetRange("Company Name", 'PW_TEST_FEW');
        RecRef.GetTable(TestBatch);

        BatchId := Coordinator
            .SetThreads(5)
            .SetBatchTimeout(30)
            .RunForRecords("PW Worker Type"::TestRecordWorker, RecRef, Payload);

        Coordinator.WaitForCompletion(BatchId);
        LibraryAssert.AreEqual(2, Coordinator.GetTotalChunks(BatchId),
            'Chunk count should equal record count');

        Coordinator.Cleanup(BatchId);
        TestBatch.Reset();
        TestBatch.SetRange("Company Name", 'PW_TEST_FEW');
        TestBatch.DeleteAll();
        Commit();
    end;

    [Test]
    procedure RunForRecordsZeroRecords()
    // [SCENARIO 8.5] RunForRecords with zero records returns null GUID
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        TestBatch: Record "PW Batch";
        RecRef: RecordRef;
        Payload: JsonObject;
        BatchId: Guid;
    begin
        TestBatch.SetRange("Company Name", 'PW_NONEXISTENT');
        RecRef.GetTable(TestBatch);

        BatchId := Coordinator
            .SetThreads(3)
            .RunForRecords("PW Worker Type"::TestRecordWorker, RecRef, Payload);

        LibraryAssert.IsTrue(IsNullGuid(BatchId), 'Zero records should return null GUID');
    end;

    #endregion

    #region Error Handling

    [Test]
    procedure WorkerErrorCaptured()
    // [SCENARIO 9.1] Worker that raises Error() — chunk marked Failed with error message
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        Chunks: List of [JsonObject];
        Chunk: JsonObject;
        Errors: List of [Text];
        BatchId: Guid;
    begin
        Chunk.Add('Data', 'test');
        Chunks.Add(Chunk);

        BatchId := Coordinator
            .SetBatchTimeout(30)
            .RunForChunks("PW Worker Type"::TestErrorWorker, Chunks);

        LibraryAssert.IsFalse(Coordinator.WaitForCompletion(BatchId), 'Should return false for failed batch');
        Coordinator.GetErrors(BatchId, Errors);
        LibraryAssert.AreEqual(1, Errors.Count(), 'Should have 1 error');

        Coordinator.Cleanup(BatchId);
        Commit();
    end;

    [Test]
    procedure PartialFailureMixedResults()
    // [SCENARIO 9.2] Some chunks succeed, some fail — PartialFailure status
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        Chunks: List of [JsonObject];
        SuccessChunk: JsonObject;
        FailChunk: JsonObject;
        Items: JsonArray;
        Results: List of [JsonObject];
        Errors: List of [Text];
        BatchId: Guid;
    begin
        Items.Add('OK-1');
        SuccessChunk.Add('$Items', Items);
        Chunks.Add(SuccessChunk);

        FailChunk.Add('$ShouldFail', true);
        Chunks.Add(FailChunk);

        BatchId := Coordinator
            .SetBatchTimeout(30)
            .RunForChunks("PW Worker Type"::TestWorker, Chunks);

        LibraryAssert.IsFalse(Coordinator.WaitForCompletion(BatchId), 'Should return false');
        LibraryAssert.IsTrue(
            Coordinator.GetStatus(BatchId) = "PW Batch Status"::PartialFailure,
            'Status should be PartialFailure');

        Coordinator.GetResults(BatchId, Results);
        LibraryAssert.AreEqual(1, Results.Count(), 'Should have 1 result from successful chunk');

        Coordinator.GetErrors(BatchId, Errors);
        LibraryAssert.AreEqual(1, Errors.Count(), 'Should have 1 error from failed chunk');

        Coordinator.Cleanup(BatchId);
        Commit();
    end;

    [Test]
    procedure AllChunksFail()
    // [SCENARIO 9.3] All chunks fail — batch status Failed
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        Chunks: List of [JsonObject];
        Chunk1: JsonObject;
        Chunk2: JsonObject;
        BatchId: Guid;
    begin
        Chunk1.Add('Data', '1');
        Chunk2.Add('Data', '2');
        Chunks.Add(Chunk1);
        Chunks.Add(Chunk2);

        BatchId := Coordinator
            .SetBatchTimeout(30)
            .RunForChunks("PW Worker Type"::TestErrorWorker, Chunks);

        Coordinator.WaitForCompletion(BatchId);
        LibraryAssert.IsTrue(
            Coordinator.GetStatus(BatchId) = "PW Batch Status"::Failed,
            'Status should be Failed');

        Coordinator.Cleanup(BatchId);
        Commit();
    end;

    [Test]
    procedure GetFailedChunkInputsForRetry()
    // [SCENARIO 9.4] GetFailedChunkInputs returns original inputs for retry
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        Chunks: List of [JsonObject];
        Chunk: JsonObject;
        FailedInputs: List of [JsonObject];
        Token: JsonToken;
        BatchId: Guid;
    begin
        Chunk.Add('DocumentNo', 'DOC-001');
        Chunks.Add(Chunk);

        BatchId := Coordinator
            .SetBatchTimeout(30)
            .RunForChunks("PW Worker Type"::TestErrorWorker, Chunks);

        Coordinator.WaitForCompletion(BatchId);
        Coordinator.GetFailedChunkInputs(BatchId, FailedInputs);
        LibraryAssert.AreEqual(1, FailedInputs.Count(), 'Should return 1 failed input');
        FailedInputs.Get(1).Get('DocumentNo', Token);
        LibraryAssert.AreEqual('DOC-001', Token.AsValue().AsText(),
            'Failed input should preserve original payload');

        Coordinator.Cleanup(BatchId);
        Commit();
    end;

    [Test]
    procedure CommitInsideWorkerCausesFailure()
    // [SCENARIO 9.5] Worker that calls Commit() fails due to CommitBehavior::Error
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        Chunks: List of [JsonObject];
        Chunk: JsonObject;
        Errors: List of [Text];
        BatchId: Guid;
    begin
        Chunk.Add('Data', 'test');
        Chunks.Add(Chunk);

        BatchId := Coordinator
            .SetBatchTimeout(30)
            .RunForChunks("PW Worker Type"::TestCommitWorker, Chunks);

        LibraryAssert.IsFalse(Coordinator.WaitForCompletion(BatchId),
            'Should fail due to CommitBehavior::Error');
        LibraryAssert.IsTrue(
            Coordinator.GetStatus(BatchId) = "PW Batch Status"::Failed,
            'Status should be Failed');

        Coordinator.GetErrors(BatchId, Errors);
        LibraryAssert.AreEqual(1, Errors.Count(), 'Should have captured error');

        Coordinator.Cleanup(BatchId);
        Commit();
    end;

    #endregion

    #region Timeout & Configuration

    [Test]
    procedure TimeoutCausesWaitToReturnFalse()
    // [SCENARIO 10.1] SetTimeout causes WaitForCompletion to return false on slow batch
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        Items: List of [Text];
        Payload: JsonObject;
        BatchId: Guid;
    begin
        Items.Add('SlowItem');
        Payload.Add('WorkDurationMs', 10000);

        BatchId := Coordinator
            .SetThreads(1)
            .SetBatchTimeout(1)
            .SetPollInterval(100)
            .RunForList("PW Worker Type"::Sample, Items, Payload);

        LibraryAssert.IsFalse(Coordinator.WaitForCompletion(BatchId), 'Should timeout');

        Coordinator.Cleanup(BatchId);
        Commit();
    end;

    [Test]
    procedure FluentBuilderChaining()
    // [SCENARIO 10.3] Fluent builder chaining works with all configuration methods
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        Items: List of [Text];
        Payload: JsonObject;
        BatchId: Guid;
    begin
        Items.Add('Item-001');

        BatchId := Coordinator
            .SetThreads(1)
            .SetBatchTimeout(30)
            .SetPollInterval(200)
            .RunForList("PW Worker Type"::TestWorker, Items, Payload);

        LibraryAssert.IsTrue(Coordinator.WaitForCompletion(BatchId), 'Chained call should work');

        Coordinator.Cleanup(BatchId);
        Commit();
    end;

    [Test]
    procedure RunForListDefaultThreadCount()
    // [SCENARIO 10.4] RunForList without SetThreads uses default of 4
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        Items: List of [Text];
        Payload: JsonObject;
        BatchId: Guid;
    begin
        Items.Add('Item-001');

        BatchId := Coordinator
            .SetBatchTimeout(30)
            .RunForList("PW Worker Type"::TestWorker, Items, Payload);

        LibraryAssert.IsTrue(Coordinator.WaitForCompletion(BatchId), 'Batch should complete with default threads');

        Coordinator.Cleanup(BatchId);
        Commit();
    end;

    [Test]
    procedure RunForRecordsUnevenSplit()
    // [SCENARIO 8.6] RunForRecords with uneven record/thread division — last chunk clamped
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        TestBatch: Record "PW Batch";
        RecRef: RecordRef;
        Payload: JsonObject;
        Results: List of [JsonObject];
        BatchId: Guid;
        ResultObj: JsonObject;
        Token: JsonToken;
        TotalRecords: Integer;
        ActualTotal: Integer;
        i: Integer;
    begin
        TestBatch.SetRange("Company Name", 'PW_TEST_UNEVEN');
        TestBatch.DeleteAll();

        // 7 records / 3 threads → last chunk's EndIdx gets clamped
        for i := 1 to 7 do begin
            Clear(TestBatch);
            TestBatch.Id := CreateGuid();
            TestBatch.Status := "PW Batch Status"::Pending;
            TestBatch."Company Name" := 'PW_TEST_UNEVEN';
            TestBatch.Insert();
        end;
        Commit();

        TestBatch.Reset();
        TestBatch.SetRange("Company Name", 'PW_TEST_UNEVEN');
        RecRef.GetTable(TestBatch);
        TotalRecords := RecRef.Count();

        BatchId := Coordinator
            .SetThreads(3)
            .SetBatchTimeout(30)
            .RunForRecords("PW Worker Type"::TestRecordWorker, RecRef, Payload);

        LibraryAssert.IsTrue(Coordinator.WaitForCompletion(BatchId), 'Batch should complete');
        Coordinator.GetResults(BatchId, Results);

        ActualTotal := 0;
        for i := 1 to Results.Count() do begin
            ResultObj := Results.Get(i);
            ResultObj.Get('RecordCount', Token);
            ActualTotal += Token.AsValue().AsInteger();
        end;
        LibraryAssert.AreEqual(TotalRecords, ActualTotal, 'Total records should match with uneven split');

        Coordinator.Cleanup(BatchId);
        TestBatch.Reset();
        TestBatch.SetRange("Company Name", 'PW_TEST_UNEVEN');
        TestBatch.DeleteAll();
        Commit();
    end;

    [Test]
    procedure GuardNoWriteTransaction()
    // [SCENARIO 10.5] RunForList errors when called inside a write transaction
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        Batch: Record "PW Batch";
        Items: List of [Text];
        Payload: JsonObject;
    begin
        // Create a write transaction by inserting a record
        Batch.Id := CreateGuid();
        Batch.Status := "PW Batch Status"::Pending;
        Batch.Insert();

        Items.Add('X');

        asserterror Coordinator.RunForList("PW Worker Type"::TestWorker, Items, Payload);

        LibraryAssert.ExpectedError('Cannot start a parallel batch while a write transaction is active');
    end;

    [Test]
    procedure SampleWorkerCompletesSuccessfully()
    // [SCENARIO 10.6] Sample worker runs to completion with short sleep
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        Items: List of [Text];
        Payload: JsonObject;
        Results: List of [JsonObject];
        BatchId: Guid;
        ResultObj: JsonObject;
        Token: JsonToken;
    begin
        Items.Add('Item-001');
        Items.Add('Item-002');
        Payload.Add('WorkDurationMs', 50);

        BatchId := Coordinator
            .SetThreads(1)
            .SetBatchTimeout(30)
            .RunForList("PW Worker Type"::Sample, Items, Payload);

        LibraryAssert.IsTrue(Coordinator.WaitForCompletion(BatchId), 'Sample worker should complete');
        Coordinator.GetResults(BatchId, Results);
        LibraryAssert.AreEqual(1, Results.Count(), 'Should have 1 result');

        ResultObj := Results.Get(1);
        ResultObj.Get('ItemsProcessed', Token);
        LibraryAssert.AreEqual(2, Token.AsValue().AsInteger(), 'Should have processed 2 items');

        Coordinator.Cleanup(BatchId);
        Commit();
    end;

    #endregion

    #region RunAndWait convenience methods

    [Test]
    procedure RunAndWaitForListSuccess()
    // [SCENARIO 11.1] RunAndWaitForList returns true and collects results on success
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        Items: List of [Text];
        Payload: JsonObject;
        Results: List of [JsonObject];
        ResultObj: JsonObject;
        Token: JsonToken;
        TotalItems: Integer;
        i: Integer;
    begin
        for i := 1 to 6 do
            Items.Add(StrSubstNo('Item-%1', i));

        LibraryAssert.IsTrue(
            Coordinator.SetThreads(2).SetBatchTimeout(30).RunAndWaitForList(
                "PW Worker Type"::TestWorker, Items, Payload, Results),
            'RunAndWaitForList should return true');

        TotalItems := 0;
        for i := 1 to Results.Count() do begin
            ResultObj := Results.Get(i);
            ResultObj.Get('ItemCount', Token);
            TotalItems += Token.AsValue().AsInteger();
        end;
        LibraryAssert.AreEqual(6, TotalItems, 'All items should be processed');

        Commit();
    end;

    [Test]
    procedure RunAndWaitForListFailure()
    // [SCENARIO 11.2] RunAndWaitForList returns false on failure, results empty
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        Chunks: List of [JsonObject];
        Chunk: JsonObject;
        Results: List of [JsonObject];
    begin
        Chunk.Add('$ShouldFail', true);
        Chunks.Add(Chunk);

        LibraryAssert.IsFalse(
            Coordinator.SetBatchTimeout(30).RunAndWaitForChunks(
                "PW Worker Type"::TestWorker, Chunks, Results),
            'Should return false on failure');

        LibraryAssert.AreEqual(0, Results.Count(), 'Results should be empty on failure');

        Commit();
    end;

    [Test]
    procedure RunAndWaitForListEmptyList()
    // [SCENARIO 11.3] RunAndWaitForList with empty list returns true
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        Items: List of [Text];
        Payload: JsonObject;
        Results: List of [JsonObject];
    begin
        LibraryAssert.IsTrue(
            Coordinator.RunAndWaitForList(
                "PW Worker Type"::TestWorker, Items, Payload, Results),
            'Empty list should return true');

        LibraryAssert.AreEqual(0, Results.Count(), 'Results should be empty');
    end;

    [Test]
    procedure RunAndWaitForRecordsSuccess()
    // [SCENARIO 11.4] RunAndWaitForRecords returns true and counts match
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        TestBatch: Record "PW Batch";
        RecRef: RecordRef;
        Payload: JsonObject;
        Results: List of [JsonObject];
        ResultObj: JsonObject;
        Token: JsonToken;
        ActualTotal: Integer;
        i: Integer;
    begin
        TestBatch.SetRange("Company Name", 'PW_TEST_RAW');
        TestBatch.DeleteAll();

        for i := 1 to 4 do begin
            Clear(TestBatch);
            TestBatch.Id := CreateGuid();
            TestBatch.Status := "PW Batch Status"::Pending;
            TestBatch."Company Name" := 'PW_TEST_RAW';
            TestBatch.Insert();
        end;
        Commit();

        TestBatch.Reset();
        TestBatch.SetRange("Company Name", 'PW_TEST_RAW');
        RecRef.GetTable(TestBatch);

        LibraryAssert.IsTrue(
            Coordinator.SetThreads(2).SetBatchTimeout(30).RunAndWaitForRecords(
                "PW Worker Type"::TestRecordWorker, RecRef, Payload, Results),
            'RunAndWaitForRecords should return true');

        ActualTotal := 0;
        for i := 1 to Results.Count() do begin
            ResultObj := Results.Get(i);
            ResultObj.Get('RecordCount', Token);
            ActualTotal += Token.AsValue().AsInteger();
        end;
        LibraryAssert.AreEqual(4, ActualTotal, 'Total records should match');

        TestBatch.Reset();
        TestBatch.SetRange("Company Name", 'PW_TEST_RAW');
        TestBatch.DeleteAll();
        Commit();
    end;

    [Test]
    procedure RunAndWaitForRecordsEmptyTable()
    // [SCENARIO 11.5] RunAndWaitForRecords with zero records returns true
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        TestBatch: Record "PW Batch";
        RecRef: RecordRef;
        Payload: JsonObject;
        Results: List of [JsonObject];
    begin
        TestBatch.SetRange("Company Name", 'PW_NONEXISTENT_RAW');
        RecRef.GetTable(TestBatch);

        LibraryAssert.IsTrue(
            Coordinator.RunAndWaitForRecords(
                "PW Worker Type"::TestRecordWorker, RecRef, Payload, Results),
            'Empty table should return true');

        LibraryAssert.AreEqual(0, Results.Count(), 'Results should be empty');
    end;

    [Test]
    procedure RunAndWaitForChunksSuccess()
    // [SCENARIO 11.6] RunAndWaitForChunks returns true and collects results
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        Chunks: List of [JsonObject];
        Chunk: JsonObject;
        Items: JsonArray;
        Results: List of [JsonObject];
    begin
        Items.Add('A');
        Items.Add('B');
        Chunk.Add('$Items', Items);
        Chunks.Add(Chunk);

        Clear(Chunk);
        Clear(Items);
        Items.Add('C');
        Chunk.Add('$Items', Items);
        Chunks.Add(Chunk);

        LibraryAssert.IsTrue(
            Coordinator.SetBatchTimeout(30).RunAndWaitForChunks(
                "PW Worker Type"::TestWorker, Chunks, Results),
            'RunAndWaitForChunks should return true');

        LibraryAssert.AreEqual(2, Results.Count(), 'Should have 2 results');

        Commit();
    end;

    [Test]
    procedure RunAndWaitForChunksEmptyList()
    // [SCENARIO 11.7] RunAndWaitForChunks with empty list returns true
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        Chunks: List of [JsonObject];
        Results: List of [JsonObject];
    begin
        LibraryAssert.IsTrue(
            Coordinator.RunAndWaitForChunks(
                "PW Worker Type"::TestWorker, Chunks, Results),
            'Empty chunks should return true');

        LibraryAssert.AreEqual(0, Results.Count(), 'Results should be empty');
    end;

    [Test]
    procedure RunAndWaitKeepsRecordsByDefault()
    // [SCENARIO 11.8] RunAndWait keeps batch records by default (AutoCleanup=false)
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        Batch: Record "PW Batch";
        Items: List of [Text];
        Results: List of [JsonObject];
        BatchCountBefore: Integer;
    begin
        Batch.Reset();
        BatchCountBefore := Batch.Count();

        Items.Add('X');
        Coordinator.SetThreads(1).SetBatchTimeout(30).RunAndWaitForList(
            "PW Worker Type"::TestWorker, Items, Results);

        Batch.Reset();
        LibraryAssert.AreEqual(BatchCountBefore + 1, Batch.Count(),
            'Batch should be kept when AutoCleanup is not set');

        // Clean up manually
        Batch.FindLast();
        Coordinator.Cleanup(Batch.Id);
        Commit();
    end;

    [Test]
    procedure RunAndWaitCleansUpWhenEnabled()
    // [SCENARIO 11.9] RunAndWait deletes batch records when AutoCleanup = true
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        Batch: Record "PW Batch";
        Items: List of [Text];
        Payload: JsonObject;
        Results: List of [JsonObject];
        BatchCountBefore: Integer;
    begin
        Batch.Reset();
        BatchCountBefore := Batch.Count();

        Items.Add('X');
        Coordinator.SetThreads(1).SetBatchTimeout(30).RunAndWaitForList(
            "PW Worker Type"::TestWorker, Items, Payload, true, Results);

        Batch.Reset();
        LibraryAssert.AreEqual(BatchCountBefore, Batch.Count(),
            'Batch should be cleaned up after RunAndWait');

        Commit();
    end;

    [Test]
    procedure RunAndWaitCleansUpAfterFailure()
    // [SCENARIO 11.10] RunAndWait deletes batch records after failure when AutoCleanup = true
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        Batch: Record "PW Batch";
        Chunks: List of [JsonObject];
        Chunk: JsonObject;
        Results: List of [JsonObject];
        BatchCountBefore: Integer;
    begin
        Batch.Reset();
        BatchCountBefore := Batch.Count();

        Chunk.Add('Data', 'test');
        Chunks.Add(Chunk);
        Coordinator.SetBatchTimeout(30).RunAndWaitForChunks(
            "PW Worker Type"::TestErrorWorker, Chunks, true, Results);

        Batch.Reset();
        LibraryAssert.AreEqual(BatchCountBefore, Batch.Count(),
            'Batch should be cleaned up even after failure');

        Commit();
    end;

    #endregion

    #region Payload-less overloads

    [Test]
    procedure RunForListWithoutPayload()
    // [SCENARIO 12.1] RunForList without payload overload works the same as with empty payload
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        Items: List of [Text];
        Results: List of [JsonObject];
        BatchId: Guid;
        ResultObj: JsonObject;
        Token: JsonToken;
        TotalItems: Integer;
        i: Integer;
    begin
        for i := 1 to 4 do
            Items.Add(StrSubstNo('Item-%1', i));

        BatchId := Coordinator
            .SetThreads(2)
            .SetBatchTimeout(30)
            .RunForList("PW Worker Type"::TestWorker, Items);

        LibraryAssert.IsTrue(Coordinator.WaitForCompletion(BatchId), 'Batch should complete');
        Coordinator.GetResults(BatchId, Results);

        TotalItems := 0;
        for i := 1 to Results.Count() do begin
            ResultObj := Results.Get(i);
            ResultObj.Get('ItemCount', Token);
            TotalItems += Token.AsValue().AsInteger();
        end;
        LibraryAssert.AreEqual(4, TotalItems, 'All items should be processed');

        Coordinator.Cleanup(BatchId);
        Commit();
    end;

    [Test]
    procedure RunForRecordsWithoutPayload()
    // [SCENARIO 12.2] RunForRecords without payload overload works correctly
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        TestBatch: Record "PW Batch";
        RecRef: RecordRef;
        Results: List of [JsonObject];
        BatchId: Guid;
        ResultObj: JsonObject;
        Token: JsonToken;
        ActualTotal: Integer;
        i: Integer;
    begin
        TestBatch.SetRange("Company Name", 'PW_TEST_NOPAY');
        TestBatch.DeleteAll();

        for i := 1 to 3 do begin
            Clear(TestBatch);
            TestBatch.Id := CreateGuid();
            TestBatch.Status := "PW Batch Status"::Pending;
            TestBatch."Company Name" := 'PW_TEST_NOPAY';
            TestBatch.Insert();
        end;
        Commit();

        TestBatch.Reset();
        TestBatch.SetRange("Company Name", 'PW_TEST_NOPAY');
        RecRef.GetTable(TestBatch);

        BatchId := Coordinator
            .SetThreads(2)
            .SetBatchTimeout(30)
            .RunForRecords("PW Worker Type"::TestRecordWorker, RecRef);

        LibraryAssert.IsTrue(Coordinator.WaitForCompletion(BatchId), 'Batch should complete');
        Coordinator.GetResults(BatchId, Results);

        ActualTotal := 0;
        for i := 1 to Results.Count() do begin
            ResultObj := Results.Get(i);
            ResultObj.Get('RecordCount', Token);
            ActualTotal += Token.AsValue().AsInteger();
        end;
        LibraryAssert.AreEqual(3, ActualTotal, 'Total records should match');

        Coordinator.Cleanup(BatchId);
        TestBatch.Reset();
        TestBatch.SetRange("Company Name", 'PW_TEST_NOPAY');
        TestBatch.DeleteAll();
        Commit();
    end;

    [Test]
    procedure RunAndWaitForListWithoutPayload()
    // [SCENARIO 12.3] RunAndWaitForList without payload returns true and collects results
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        Items: List of [Text];
        Results: List of [JsonObject];
        ResultObj: JsonObject;
        Token: JsonToken;
        TotalItems: Integer;
        i: Integer;
    begin
        for i := 1 to 6 do
            Items.Add(StrSubstNo('Item-%1', i));

        LibraryAssert.IsTrue(
            Coordinator.SetThreads(2).SetBatchTimeout(30).RunAndWaitForList(
                "PW Worker Type"::TestWorker, Items, Results),
            'RunAndWaitForList without payload should return true');

        TotalItems := 0;
        for i := 1 to Results.Count() do begin
            ResultObj := Results.Get(i);
            ResultObj.Get('ItemCount', Token);
            TotalItems += Token.AsValue().AsInteger();
        end;
        LibraryAssert.AreEqual(6, TotalItems, 'All items should be processed');

        Commit();
    end;

    [Test]
    procedure RunAndWaitForListWithoutPayloadEmptyList()
    // [SCENARIO 12.4] RunAndWaitForList without payload with empty list returns true
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        Items: List of [Text];
        Results: List of [JsonObject];
    begin
        LibraryAssert.IsTrue(
            Coordinator.RunAndWaitForList(
                "PW Worker Type"::TestWorker, Items, Results),
            'Empty list should return true');

        LibraryAssert.AreEqual(0, Results.Count(), 'Results should be empty');
    end;

    [Test]
    procedure RunAndWaitForRecordsWithoutPayload()
    // [SCENARIO 12.5] RunAndWaitForRecords without payload returns true and counts match
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        TestBatch: Record "PW Batch";
        RecRef: RecordRef;
        Results: List of [JsonObject];
        ResultObj: JsonObject;
        Token: JsonToken;
        ActualTotal: Integer;
        i: Integer;
    begin
        TestBatch.SetRange("Company Name", 'PW_TEST_RAW_NP');
        TestBatch.DeleteAll();

        for i := 1 to 4 do begin
            Clear(TestBatch);
            TestBatch.Id := CreateGuid();
            TestBatch.Status := "PW Batch Status"::Pending;
            TestBatch."Company Name" := 'PW_TEST_RAW_NP';
            TestBatch.Insert();
        end;
        Commit();

        TestBatch.Reset();
        TestBatch.SetRange("Company Name", 'PW_TEST_RAW_NP');
        RecRef.GetTable(TestBatch);

        LibraryAssert.IsTrue(
            Coordinator.SetThreads(2).SetBatchTimeout(30).RunAndWaitForRecords(
                "PW Worker Type"::TestRecordWorker, RecRef, Results),
            'RunAndWaitForRecords without payload should return true');

        ActualTotal := 0;
        for i := 1 to Results.Count() do begin
            ResultObj := Results.Get(i);
            ResultObj.Get('RecordCount', Token);
            ActualTotal += Token.AsValue().AsInteger();
        end;
        LibraryAssert.AreEqual(4, ActualTotal, 'Total records should match');

        TestBatch.Reset();
        TestBatch.SetRange("Company Name", 'PW_TEST_RAW_NP');
        TestBatch.DeleteAll();
        Commit();
    end;

    #endregion

    #region Session timeout

    [Test]
    procedure SessionTimeoutKillsHungWorker()
    // [SCENARIO 13.1] SetSessionTimeout kills a worker that exceeds the limit.
    // Worker sleeps 10s, session timeout is 2s — platform kills the session.
    // WaitForCompletion detects the dead session via IsSessionActive,
    // marks the chunk as Failed, and recounts batch counters.
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        Items: List of [Text];
        Payload: JsonObject;
        Errors: List of [Text];
        BatchId: Guid;
    begin
        Items.Add('HungItem');
        Payload.Add('WorkDurationMs', 10000);

        BatchId := Coordinator
            .SetThreads(1)
            .SetBatchTimeout(10)
            .SetSessionTimeout(2000)
            .RunForList("PW Worker Type"::Sample, Items, Payload);

        // WaitForCompletion detects the dead session after session timeout + margin,
        // marks it Failed, and returns false.
        LibraryAssert.IsFalse(Coordinator.WaitForCompletion(BatchId),
            'Batch should fail — session was killed by timeout');
        LibraryAssert.IsTrue(
            Coordinator.GetStatus(BatchId) = "PW Batch Status"::Failed,
            'Status should be Failed');

        Coordinator.GetErrors(BatchId, Errors);
        LibraryAssert.AreEqual(1, Errors.Count(), 'Should have 1 error from killed session');

        Coordinator.Cleanup(BatchId);
        Commit();
    end;

    [Test]
    procedure SessionTimeoutDoesNotKillFastWorker()
    // [SCENARIO 13.2] SetSessionTimeout does not affect workers that finish in time.
    // Worker sleeps 50ms per item, session timeout is 30s — completes normally.
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        Items: List of [Text];
        Payload: JsonObject;
        Results: List of [JsonObject];
        BatchId: Guid;
    begin
        Items.Add('FastItem');
        Payload.Add('WorkDurationMs', 50);

        BatchId := Coordinator
            .SetThreads(1)
            .SetBatchTimeout(30)
            .SetSessionTimeout(30000)
            .RunForList("PW Worker Type"::Sample, Items, Payload);

        LibraryAssert.IsTrue(Coordinator.WaitForCompletion(BatchId),
            'Batch should complete — worker finished within session timeout');

        Coordinator.GetResults(BatchId, Results);
        LibraryAssert.AreEqual(1, Results.Count(), 'Should have 1 result');

        Coordinator.Cleanup(BatchId);
        Commit();
    end;

    [Test]
    procedure BatchTimeoutReturnsFalseButSessionContinues()
    // [SCENARIO 13.3] SetBatchTimeout expires before the worker finishes,
    // but SetSessionTimeout is longer — WaitForCompletion returns false while session still runs.
    // After waiting a bit more, the session finishes and batch reaches terminal status.
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        Items: List of [Text];
        Payload: JsonObject;
        BatchId: Guid;
    begin
        Items.Add('SlowItem');
        Payload.Add('WorkDurationMs', 5000);

        BatchId := Coordinator
            .SetThreads(1)
            .SetBatchTimeout(1)
            .SetSessionTimeout(30000)
            .SetPollInterval(100)
            .RunForList("PW Worker Type"::Sample, Items, Payload);

        // Batch timeout expires — returns false, but session is still running
        LibraryAssert.IsFalse(Coordinator.WaitForCompletion(BatchId),
            'Should return false — batch timeout expired');
        LibraryAssert.IsFalse(Coordinator.IsFinished(BatchId),
            'Batch should still be running — session timeout has not expired');

        // Wait for the session to actually finish (5s worker + margin)
        Sleep(6000);

        // Now the session should have finished
        LibraryAssert.IsTrue(Coordinator.IsFinished(BatchId),
            'Batch should be finished after session completes');
        LibraryAssert.IsTrue(
            Coordinator.GetStatus(BatchId) = "PW Batch Status"::Completed,
            'Status should be Completed — session finished successfully');

        Coordinator.Cleanup(BatchId);
        Commit();
    end;

    #endregion
}
