codeunit 99203 "PW Batch Cleanup Test"
{
    Subtype = Test;
    TestPermissions = Disabled;

    var
        LibraryAssert: Codeunit "Library Assert";
        TestHelper: Codeunit "PW Test Helper";

    [Test]
    procedure CleanupsOldCompletedBatch()
    // [SCENARIO 5.1] Deletes Completed batch older than threshold
    var
        Cleanup: Codeunit "PW Batch Cleanup";
        Batch: Record "PW Batch";
        Chunk: Record "PW Batch Chunk";
        BatchId: Guid;
    begin
        BatchId := TestHelper.CreateBatchWithTimestamp(
            "PW Batch Status"::Completed,
            CurrentDateTime() - (48 * 3600000));
        TestHelper.CreateSimpleChunk(BatchId, 1, "PW Chunk Status"::Completed);

        Cleanup.CleanupOlderThan(24);

        LibraryAssert.IsFalse(Batch.Get(BatchId), 'Old completed batch should be deleted');
        Chunk.SetRange("Batch Id", BatchId);
        LibraryAssert.AreEqual(0, Chunk.Count(), 'Chunks should be deleted');
    end;

    [Test]
    procedure CleanupsOldFailedBatch()
    // [SCENARIO 5.2] Deletes Failed batch older than threshold
    var
        Cleanup: Codeunit "PW Batch Cleanup";
        Batch: Record "PW Batch";
        BatchId: Guid;
    begin
        BatchId := TestHelper.CreateBatchWithTimestamp(
            "PW Batch Status"::Failed,
            CurrentDateTime() - (48 * 3600000));

        Cleanup.CleanupOlderThan(24);

        LibraryAssert.IsFalse(Batch.Get(BatchId), 'Old failed batch should be deleted');
    end;

    [Test]
    procedure CleanupsOldPartialFailureBatch()
    // [SCENARIO 5.3] Deletes PartialFailure batch older than threshold
    var
        Cleanup: Codeunit "PW Batch Cleanup";
        Batch: Record "PW Batch";
        BatchId: Guid;
    begin
        BatchId := TestHelper.CreateBatchWithTimestamp(
            "PW Batch Status"::PartialFailure,
            CurrentDateTime() - (48 * 3600000));

        Cleanup.CleanupOlderThan(24);

        LibraryAssert.IsFalse(Batch.Get(BatchId), 'Old partial failure batch should be deleted');
    end;

    [Test]
    procedure DoesNotDeleteRunningBatch()
    // [SCENARIO 5.4] Does not delete Running batch
    var
        Cleanup: Codeunit "PW Batch Cleanup";
        Batch: Record "PW Batch";
        BatchId: Guid;
    begin
        BatchId := TestHelper.CreateBatch("PW Batch Status"::Running);

        Cleanup.CleanupOlderThan(24);

        LibraryAssert.IsTrue(Batch.Get(BatchId), 'Running batch should not be deleted');
    end;

    [Test]
    procedure DoesNotDeletePendingBatch()
    // [SCENARIO 5.5] Does not delete Pending batch
    var
        Cleanup: Codeunit "PW Batch Cleanup";
        Batch: Record "PW Batch";
        BatchId: Guid;
    begin
        BatchId := TestHelper.CreateBatch("PW Batch Status"::Pending);

        Cleanup.CleanupOlderThan(24);

        LibraryAssert.IsTrue(Batch.Get(BatchId), 'Pending batch should not be deleted');
    end;

    [Test]
    procedure DoesNotDeleteRecentCompletedBatch()
    // [SCENARIO 5.6] Does not delete recent Completed batch within threshold
    var
        Cleanup: Codeunit "PW Batch Cleanup";
        Batch: Record "PW Batch";
        BatchId: Guid;
    begin
        BatchId := TestHelper.CreateBatchWithTimestamp(
            "PW Batch Status"::Completed,
            CurrentDateTime() - (1 * 3600000));

        Cleanup.CleanupOlderThan(24);

        LibraryAssert.IsTrue(Batch.Get(BatchId), 'Recent completed batch should not be deleted');
    end;

    [Test]
    procedure OnRunTriggerCleansUp24Hours()
    // [SCENARIO 5.8] OnRun trigger calls CleanupOlderThan(24)
    var
        Cleanup: Codeunit "PW Batch Cleanup";
        Batch: Record "PW Batch";
        BatchId: Guid;
    begin
        BatchId := TestHelper.CreateBatchWithTimestamp(
            "PW Batch Status"::Completed,
            CurrentDateTime() - (48 * 3600000));

        Cleanup.Run();

        LibraryAssert.IsFalse(Batch.Get(BatchId), 'OnRun should delete old completed batch');
    end;

    [Test]
    procedure MixedScenarioDeletesOnlyOldFinished()
    // [SCENARIO 5.7] Mixed — only old finished batches are deleted
    var
        Cleanup: Codeunit "PW Batch Cleanup";
        Batch: Record "PW Batch";
        OldCompletedId: Guid;
        RunningId: Guid;
        RecentCompletedId: Guid;
    begin
        OldCompletedId := TestHelper.CreateBatchWithTimestamp(
            "PW Batch Status"::Completed,
            CurrentDateTime() - (48 * 3600000));
        TestHelper.CreateSimpleChunk(OldCompletedId, 1, "PW Chunk Status"::Completed);

        RunningId := TestHelper.CreateBatch("PW Batch Status"::Running);
        TestHelper.CreateSimpleChunk(RunningId, 1, "PW Chunk Status"::Running);

        RecentCompletedId := TestHelper.CreateBatchWithTimestamp(
            "PW Batch Status"::Completed,
            CurrentDateTime() - (1 * 3600000));
        TestHelper.CreateSimpleChunk(RecentCompletedId, 1, "PW Chunk Status"::Completed);

        Cleanup.CleanupOlderThan(24);

        LibraryAssert.IsFalse(Batch.Get(OldCompletedId), 'Old completed batch should be deleted');
        LibraryAssert.IsTrue(Batch.Get(RunningId), 'Running batch should remain');
        LibraryAssert.IsTrue(Batch.Get(RecentCompletedId), 'Recent completed batch should remain');
    end;
}
