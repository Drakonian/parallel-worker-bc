namespace VolodymyrDvernytskyi.ParallelWorker.Test;

using VolodymyrDvernytskyi.ParallelWorker;
using System.TestLibraries.Utilities;

codeunit 99202 "PW Coordinator Test"
{
    Subtype = Test;
    TestPermissions = Disabled;

    var
        LibraryAssert: Codeunit "Library Assert";
        TestHelper: Codeunit "PW Test Helper";

    #region Status & Query Methods

    [Test]
    procedure GetStatusReturnsRunning()
    // [SCENARIO 2.1] GetStatus on Running batch returns Running
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        BatchId: Guid;
    begin
        BatchId := TestHelper.CreateBatch("PW Batch Status"::Running);

        LibraryAssert.IsTrue(
            Coordinator.GetStatus(BatchId) = "PW Batch Status"::Running,
            'Status should be Running');
    end;

    [Test]
    procedure GetStatusReturnsCompleted()
    // [SCENARIO 2.2] GetStatus on Completed batch
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        BatchId: Guid;
    begin
        BatchId := TestHelper.CreateBatch("PW Batch Status"::Completed);

        LibraryAssert.IsTrue(
            Coordinator.GetStatus(BatchId) = "PW Batch Status"::Completed,
            'Status should be Completed');
    end;

    [Test]
    procedure GetStatusReturnsFailed()
    // [SCENARIO 2.3] GetStatus on Failed batch
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        BatchId: Guid;
    begin
        BatchId := TestHelper.CreateBatch("PW Batch Status"::Failed);

        LibraryAssert.IsTrue(
            Coordinator.GetStatus(BatchId) = "PW Batch Status"::Failed,
            'Status should be Failed');
    end;

    [Test]
    procedure GetStatusReturnsPartialFailure()
    // [SCENARIO 2.4] GetStatus on PartialFailure batch
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        BatchId: Guid;
    begin
        BatchId := TestHelper.CreateBatch("PW Batch Status"::PartialFailure);

        LibraryAssert.IsTrue(
            Coordinator.GetStatus(BatchId) = "PW Batch Status"::PartialFailure,
            'Status should be PartialFailure');
    end;

    [Test]
    procedure GetStatusReturnsFailedForNonExistentBatch()
    // [SCENARIO 2.5] GetStatus with non-existent BatchId returns Failed
    var
        Coordinator: Codeunit "PW Batch Coordinator";
    begin
        LibraryAssert.IsTrue(
            Coordinator.GetStatus(CreateGuid()) = "PW Batch Status"::Failed,
            'Non-existent batch should return Failed');
    end;

    [Test]
    procedure GetCompletedChunksReturnsCorrectCount()
    // [SCENARIO 2.6] GetCompletedChunks returns the stored count
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        BatchId: Guid;
    begin
        BatchId := TestHelper.CreateBatchEx("PW Batch Status"::Running, 5, 3, 0);

        LibraryAssert.AreEqual(3, Coordinator.GetCompletedChunks(BatchId), 'Completed chunks should be 3');
    end;

    [Test]
    procedure GetTotalChunksReturnsCorrectCount()
    // [SCENARIO 2.7] GetTotalChunks returns the stored count
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        BatchId: Guid;
    begin
        BatchId := TestHelper.CreateBatchEx("PW Batch Status"::Running, 5, 0, 0);

        LibraryAssert.AreEqual(5, Coordinator.GetTotalChunks(BatchId), 'Total chunks should be 5');
    end;

    [Test]
    procedure IsFinishedTrueForCompleted()
    // [SCENARIO 2.8] IsFinished returns true for Completed
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        BatchId: Guid;
    begin
        BatchId := TestHelper.CreateBatch("PW Batch Status"::Completed);

        LibraryAssert.IsTrue(Coordinator.IsFinished(BatchId), 'Completed batch should be finished');
    end;

    [Test]
    procedure IsFinishedTrueForFailed()
    // [SCENARIO 2.9] IsFinished returns true for Failed
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        BatchId: Guid;
    begin
        BatchId := TestHelper.CreateBatch("PW Batch Status"::Failed);

        LibraryAssert.IsTrue(Coordinator.IsFinished(BatchId), 'Failed batch should be finished');
    end;

    [Test]
    procedure IsFinishedTrueForPartialFailure()
    // [SCENARIO 2.10] IsFinished returns true for PartialFailure
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        BatchId: Guid;
    begin
        BatchId := TestHelper.CreateBatch("PW Batch Status"::PartialFailure);

        LibraryAssert.IsTrue(Coordinator.IsFinished(BatchId), 'PartialFailure batch should be finished');
    end;

    [Test]
    procedure IsFinishedFalseForRunning()
    // [SCENARIO 2.11] IsFinished returns false for Running
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        BatchId: Guid;
    begin
        BatchId := TestHelper.CreateBatch("PW Batch Status"::Running);

        LibraryAssert.IsFalse(Coordinator.IsFinished(BatchId), 'Running batch should not be finished');
    end;

    [Test]
    procedure IsFinishedFalseForPending()
    // [SCENARIO 2.12] IsFinished returns false for Pending
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        BatchId: Guid;
    begin
        BatchId := TestHelper.CreateBatch("PW Batch Status"::Pending);

        LibraryAssert.IsFalse(Coordinator.IsFinished(BatchId), 'Pending batch should not be finished');
    end;

    [Test]
    procedure IsFinishedTrueForNonExistentBatch()
    // [SCENARIO 2.13] IsFinished with non-existent BatchId returns true
    var
        Coordinator: Codeunit "PW Batch Coordinator";
    begin
        LibraryAssert.IsTrue(Coordinator.IsFinished(CreateGuid()), 'Non-existent batch should be considered finished');
    end;

    [Test]
    procedure WaitForCompletionReturnsTrueForCompletedBatch()
    // [SCENARIO 2.14] WaitForCompletion on already-Completed batch returns true
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        BatchId: Guid;
    begin
        BatchId := TestHelper.CreateBatch("PW Batch Status"::Completed);

        LibraryAssert.IsTrue(Coordinator.WaitForCompletion(BatchId), 'Should return true for completed batch');
    end;

    [Test]
    procedure WaitForCompletionReturnsFalseForFailedBatch()
    // [SCENARIO 2.15] WaitForCompletion on already-Failed batch returns false
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        BatchId: Guid;
    begin
        BatchId := TestHelper.CreateBatch("PW Batch Status"::Failed);

        LibraryAssert.IsFalse(Coordinator.WaitForCompletion(BatchId), 'Should return false for failed batch');
    end;

    [Test]
    procedure WaitForCompletionReturnsTrueForNullGuid()
    // [SCENARIO 2.16] WaitForCompletion with null GUID returns true
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        NullId: Guid;
    begin
        LibraryAssert.IsTrue(Coordinator.WaitForCompletion(NullId), 'Should return true for null GUID');
    end;

    [Test]
    procedure WaitForCompletionReturnsFalseForNonExistentBatch()
    // [SCENARIO 2.17] WaitForCompletion with non-existent non-null batch returns false
    var
        Coordinator: Codeunit "PW Batch Coordinator";
    begin
        LibraryAssert.IsFalse(Coordinator.WaitForCompletion(CreateGuid()),
            'Should return false for non-existent batch');
    end;

    [Test]
    procedure GetCompletedChunksReturnsZeroForNonExistentBatch()
    // [SCENARIO 2.18] GetCompletedChunks with non-existent batch returns 0
    var
        Coordinator: Codeunit "PW Batch Coordinator";
    begin
        LibraryAssert.AreEqual(0, Coordinator.GetCompletedChunks(CreateGuid()),
            'Non-existent batch should return 0 completed chunks');
    end;

    [Test]
    procedure GetTotalChunksReturnsZeroForNonExistentBatch()
    // [SCENARIO 2.19] GetTotalChunks with non-existent batch returns 0
    var
        Coordinator: Codeunit "PW Batch Coordinator";
    begin
        LibraryAssert.AreEqual(0, Coordinator.GetTotalChunks(CreateGuid()),
            'Non-existent batch should return 0 total chunks');
    end;

    #endregion

    #region Result Collection

    [Test]
    procedure GetResultsSingleChunkSingleResult()
    // [SCENARIO 3.1] GetResults — single completed chunk with 1 result
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        BatchId: Guid;
        ResultObj: JsonObject;
        ResultArr: JsonArray;
        Results: List of [JsonObject];
    begin
        BatchId := TestHelper.CreateBatch("PW Batch Status"::Completed);
        ResultObj.Add('Value', 42);
        ResultArr.Add(ResultObj);
        TestHelper.CreateChunkWithResult(BatchId, 1, ResultArr);

        Coordinator.GetResults(BatchId, Results);

        LibraryAssert.AreEqual(1, Results.Count(), 'Should return 1 result');
    end;

    [Test]
    procedure GetResultsMultipleChunks()
    // [SCENARIO 3.2] GetResults — multiple completed chunks aggregated
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        BatchId: Guid;
        Result1: JsonObject;
        Result2: JsonObject;
        Arr1: JsonArray;
        Arr2: JsonArray;
        Results: List of [JsonObject];
    begin
        BatchId := TestHelper.CreateBatch("PW Batch Status"::Completed);
        Result1.Add('Chunk', 1);
        Arr1.Add(Result1);
        TestHelper.CreateChunkWithResult(BatchId, 1, Arr1);
        Result2.Add('Chunk', 2);
        Arr2.Add(Result2);
        TestHelper.CreateChunkWithResult(BatchId, 2, Arr2);

        Coordinator.GetResults(BatchId, Results);

        LibraryAssert.AreEqual(2, Results.Count(), 'Should return 2 results');
    end;

    [Test]
    procedure GetResultsFlattenMultipleResultsPerChunk()
    // [SCENARIO 3.3] GetResults — multiple results per chunk are flattened
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        BatchId: Guid;
        Obj1: JsonObject;
        Obj2: JsonObject;
        Obj3: JsonObject;
        Arr: JsonArray;
        Results: List of [JsonObject];
    begin
        BatchId := TestHelper.CreateBatch("PW Batch Status"::Completed);
        Obj1.Add('Row', 1);
        Obj2.Add('Row', 2);
        Obj3.Add('Row', 3);
        Arr.Add(Obj1);
        Arr.Add(Obj2);
        Arr.Add(Obj3);
        TestHelper.CreateChunkWithResult(BatchId, 1, Arr);

        Coordinator.GetResults(BatchId, Results);

        LibraryAssert.AreEqual(3, Results.Count(), 'Should return 3 results');
    end;

    [Test]
    procedure GetResultsSkipsFailedChunks()
    // [SCENARIO 3.4] GetResults — only returns completed chunk results
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        BatchId: Guid;
        ResultObj: JsonObject;
        ResultArr: JsonArray;
        InputPayload: JsonObject;
        Results: List of [JsonObject];
    begin
        BatchId := TestHelper.CreateBatch("PW Batch Status"::PartialFailure);
        ResultObj.Add('Value', 1);
        ResultArr.Add(ResultObj);
        TestHelper.CreateChunkWithResult(BatchId, 1, ResultArr);
        TestHelper.CreateChunkWithError(BatchId, 2, 'Some error', InputPayload);

        Coordinator.GetResults(BatchId, Results);

        LibraryAssert.AreEqual(1, Results.Count(), 'Should return only 1 result');
    end;

    [Test]
    procedure GetResultsReturnsEmptyForNoCompletedChunks()
    // [SCENARIO 3.5] GetResults — no completed chunks returns empty list
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        BatchId: Guid;
        InputPayload: JsonObject;
        Results: List of [JsonObject];
    begin
        BatchId := TestHelper.CreateBatch("PW Batch Status"::Failed);
        TestHelper.CreateChunkWithError(BatchId, 1, 'Error 1', InputPayload);

        Coordinator.GetResults(BatchId, Results);

        LibraryAssert.AreEqual(0, Results.Count(), 'Should return 0 results');
    end;

    [Test]
    procedure GetErrorsSingleFailedChunk()
    // [SCENARIO 3.6] GetErrors — single failed chunk
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        BatchId: Guid;
        InputPayload: JsonObject;
        Errors: List of [Text];
    begin
        BatchId := TestHelper.CreateBatch("PW Batch Status"::Failed);
        TestHelper.CreateChunkWithError(BatchId, 1, 'Something went wrong', InputPayload);

        Coordinator.GetErrors(BatchId, Errors);

        LibraryAssert.AreEqual(1, Errors.Count(), 'Should return 1 error');
        LibraryAssert.AreEqual('Something went wrong', Errors.Get(1), 'Error message should match');
    end;

    [Test]
    procedure GetErrorsMultipleFailedChunks()
    // [SCENARIO 3.7] GetErrors — multiple failed chunks
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        BatchId: Guid;
        InputPayload: JsonObject;
        Errors: List of [Text];
    begin
        BatchId := TestHelper.CreateBatch("PW Batch Status"::Failed);
        TestHelper.CreateChunkWithError(BatchId, 1, 'Error A', InputPayload);
        TestHelper.CreateChunkWithError(BatchId, 2, 'Error B', InputPayload);

        Coordinator.GetErrors(BatchId, Errors);

        LibraryAssert.AreEqual(2, Errors.Count(), 'Should return 2 errors');
    end;

    [Test]
    procedure GetErrorsSkipsCompletedChunks()
    // [SCENARIO 3.8] GetErrors — only returns failed chunk errors
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        BatchId: Guid;
        ResultArr: JsonArray;
        ResultObj: JsonObject;
        InputPayload: JsonObject;
        Errors: List of [Text];
    begin
        BatchId := TestHelper.CreateBatch("PW Batch Status"::PartialFailure);
        ResultObj.Add('Value', 1);
        ResultArr.Add(ResultObj);
        TestHelper.CreateChunkWithResult(BatchId, 1, ResultArr);
        TestHelper.CreateChunkWithError(BatchId, 2, 'Error only', InputPayload);

        Coordinator.GetErrors(BatchId, Errors);

        LibraryAssert.AreEqual(1, Errors.Count(), 'Should return 1 error');
    end;

    [Test]
    procedure GetErrorsReturnsEmptyForNoFailedChunks()
    // [SCENARIO 3.9] GetErrors — no failed chunks returns empty list
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        BatchId: Guid;
        ResultArr: JsonArray;
        ResultObj: JsonObject;
        Errors: List of [Text];
    begin
        BatchId := TestHelper.CreateBatch("PW Batch Status"::Completed);
        ResultObj.Add('Value', 1);
        ResultArr.Add(ResultObj);
        TestHelper.CreateChunkWithResult(BatchId, 1, ResultArr);

        Coordinator.GetErrors(BatchId, Errors);

        LibraryAssert.AreEqual(0, Errors.Count(), 'Should return 0 errors');
    end;

    [Test]
    procedure GetFailedChunkInputsReturnsOriginalPayload()
    // [SCENARIO 3.10] GetFailedChunkInputs — returns original input payload
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        BatchId: Guid;
        InputPayload: JsonObject;
        FailedInputs: List of [JsonObject];
        Token: JsonToken;
    begin
        BatchId := TestHelper.CreateBatch("PW Batch Status"::Failed);
        InputPayload.Add('DocumentNo', 'INV-001');
        TestHelper.CreateChunkWithError(BatchId, 1, 'Post failed', InputPayload);

        Coordinator.GetFailedChunkInputs(BatchId, FailedInputs);

        LibraryAssert.AreEqual(1, FailedInputs.Count(), 'Should return 1 failed input');
        FailedInputs.Get(1).Get('DocumentNo', Token);
        LibraryAssert.AreEqual('INV-001', Token.AsValue().AsText(), 'Input payload should match');
    end;

    [Test]
    procedure GetFailedChunkInputsSkipsCompletedChunks()
    // [SCENARIO 3.11] GetFailedChunkInputs — skips completed chunks
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        BatchId: Guid;
        ResultArr: JsonArray;
        ResultObj: JsonObject;
        InputPayload: JsonObject;
        FailedInputs: List of [JsonObject];
    begin
        BatchId := TestHelper.CreateBatch("PW Batch Status"::PartialFailure);
        ResultObj.Add('Value', 1);
        ResultArr.Add(ResultObj);
        TestHelper.CreateChunkWithResult(BatchId, 1, ResultArr);
        InputPayload.Add('Key', 'FailedData');
        TestHelper.CreateChunkWithError(BatchId, 2, 'Error', InputPayload);

        Coordinator.GetFailedChunkInputs(BatchId, FailedInputs);

        LibraryAssert.AreEqual(1, FailedInputs.Count(), 'Should return 1 failed input');
    end;

    [Test]
    procedure GetFailedChunkInputsReturnsEmptyForNoFailedChunks()
    // [SCENARIO 3.12] GetFailedChunkInputs — no failed chunks returns empty list
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        BatchId: Guid;
        ResultArr: JsonArray;
        ResultObj: JsonObject;
        FailedInputs: List of [JsonObject];
    begin
        BatchId := TestHelper.CreateBatch("PW Batch Status"::Completed);
        ResultObj.Add('Value', 1);
        ResultArr.Add(ResultObj);
        TestHelper.CreateChunkWithResult(BatchId, 1, ResultArr);

        Coordinator.GetFailedChunkInputs(BatchId, FailedInputs);

        LibraryAssert.AreEqual(0, FailedInputs.Count(), 'Should return 0 failed inputs');
    end;

    #endregion

    #region Cleanup

    [Test]
    procedure CleanupDeletesBatchRecord()
    // [SCENARIO 4.1] Cleanup deletes the batch record
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        Batch: Record "PW Batch";
        BatchId: Guid;
    begin
        BatchId := TestHelper.CreateBatch("PW Batch Status"::Completed);

        Coordinator.Cleanup(BatchId);

        LibraryAssert.IsFalse(Batch.Get(BatchId), 'Batch should be deleted');
    end;

    [Test]
    procedure CleanupDeletesAllChunkRecords()
    // [SCENARIO 4.2] Cleanup deletes all chunk records for that batch
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        Chunk: Record "PW Batch Chunk";
        BatchId: Guid;
    begin
        BatchId := TestHelper.CreateBatch("PW Batch Status"::Completed);
        TestHelper.CreateSimpleChunk(BatchId, 1, "PW Chunk Status"::Completed);
        TestHelper.CreateSimpleChunk(BatchId, 2, "PW Chunk Status"::Completed);
        TestHelper.CreateSimpleChunk(BatchId, 3, "PW Chunk Status"::Completed);

        Coordinator.Cleanup(BatchId);

        Chunk.SetRange("Batch Id", BatchId);
        LibraryAssert.AreEqual(0, Chunk.Count(), 'All chunks should be deleted');
    end;

    [Test]
    procedure CleanupDoesNotAffectOtherBatches()
    // [SCENARIO 4.3] Cleanup does not affect other batches
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        Batch: Record "PW Batch";
        Chunk: Record "PW Batch Chunk";
        BatchId1: Guid;
        BatchId2: Guid;
    begin
        BatchId1 := TestHelper.CreateBatch("PW Batch Status"::Completed);
        TestHelper.CreateSimpleChunk(BatchId1, 1, "PW Chunk Status"::Completed);
        BatchId2 := TestHelper.CreateBatch("PW Batch Status"::Completed);
        TestHelper.CreateSimpleChunk(BatchId2, 1, "PW Chunk Status"::Completed);

        Coordinator.Cleanup(BatchId1);

        LibraryAssert.IsTrue(Batch.Get(BatchId2), 'Batch 2 should still exist');
        Chunk.SetRange("Batch Id", BatchId2);
        LibraryAssert.AreEqual(1, Chunk.Count(), 'Batch 2 chunks should still exist');
    end;

    #endregion
}
