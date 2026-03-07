namespace VolodymyrDvernytskyi.ParallelWorker;

codeunit 99002 "PW Task Dispatcher"
{
    Access = Internal;
    TableNo = "PW Batch Chunk";

    trigger OnRun()
    var
        Batch: Record "PW Batch";
        ChunkContext: Codeunit "PW Chunk Context";
        ErrorText: Text;
        ErrorCallStack: Text;
    begin
        if not Rec.Get(Rec."Batch Id", Rec."Chunk Index") then
            exit;

        Batch.Get(Rec."Batch Id");

        Rec.Status := "PW Chunk Status"::Running;
        Rec."Session Id" := SessionId();
        Rec."Started At" := CurrentDateTime();
        Rec.Modify();
        // Must commit Running status before TryFunction.
        // TryFunction rolls back on worker failure — without this commit,
        // the Running status would be lost and we couldn't distinguish
        // "never started" from "started and failed".
        Commit();

        ChunkContext.Init(Rec);

        ClearLastError();
        if TryExecuteWorker(Batch."Worker Type", ChunkContext) then begin
            ChunkContext.SaveResultsTo(Rec);
            Rec.Status := "PW Chunk Status"::Completed;
            Rec."Completed At" := CurrentDateTime();
            Rec.Modify();
            // Persist chunk's Completed status before updating batch counters.
            // If UpdateBatchCounters fails (e.g. lock timeout on PW Batch),
            // the chunk is still correctly marked as Completed.
            // The next finishing chunk will recompute counters correctly.
            Commit();
        end else begin
            ErrorText := GetLastErrorText();
            ErrorCallStack := GetLastErrorCallStack();
            Rec.Get(Rec."Batch Id", Rec."Chunk Index");
            Rec."Error Message" := CopyStr(ErrorText, 1, MaxStrLen(Rec."Error Message"));
            WriteErrorCallStack(Rec, ErrorCallStack);
            Rec.Status := "PW Chunk Status"::Failed;
            Rec."Completed At" := CurrentDateTime();
            Rec.Modify();
            // Same safety as success path — persist Failed status
            // before attempting batch counter update.
            Commit();
        end;

        UpdateBatchCounters(Rec."Batch Id");
    end;

    [TryFunction]
    [CommitBehavior(CommitBehavior::Error)]
    local procedure TryExecuteWorker(WorkerType: Enum "PW Worker Type"; var Context: Codeunit "PW Chunk Context")
    var
        Worker: Interface "PW IParallel Worker";
    begin
        // CommitBehavior::Error prevents workers from calling Commit() inside Execute.
        // If a worker calls Commit(), a runtime error is raised immediately.
        // TryFunction catches it and the chunk is marked Failed with a clear error.
        // This enforces the "no Commit inside workers" rule at the platform level.
        Worker := WorkerType;
        Worker.Execute(Context);
    end;

    local procedure WriteErrorCallStack(var Chunk: Record "PW Batch Chunk"; CallStack: Text)
    var
        OutStream: OutStream;
    begin
        Chunk."Error Call Stack".CreateOutStream(OutStream, TextEncoding::UTF8);
        OutStream.WriteText(CallStack);
    end;

    local procedure UpdateBatchCounters(BatchId: Guid)
    var
        Batch: Record "PW Batch";
        Chunk: Record "PW Batch Chunk";
        CompletedCount: Integer;
        FailedCount: Integer;
    begin
        Batch.LockTable();
        Batch.Get(BatchId);

        Chunk.SetRange("Batch Id", BatchId);
        Chunk.SetRange(Status, "PW Chunk Status"::Completed);
        CompletedCount := Chunk.Count();

        Chunk.SetRange(Status, "PW Chunk Status"::Failed);
        FailedCount := Chunk.Count();

        Batch."Completed Chunks" := CompletedCount;
        Batch."Failed Chunks" := FailedCount;

        if (CompletedCount + FailedCount) = Batch."Total Chunks" then begin
            Batch."Completed At" := CurrentDateTime();
            if FailedCount = 0 then
                Batch.Status := "PW Batch Status"::Completed
            else if CompletedCount = 0 then
                Batch.Status := "PW Batch Status"::Failed
            else
                Batch.Status := "PW Batch Status"::PartialFailure;
        end;

        Batch.Modify();
        // Release the LockTable lock acquired above.
        // Without this commit, the lock is held until the session ends,
        // blocking other chunks that are finishing concurrently.
        Commit();
    end;
}
