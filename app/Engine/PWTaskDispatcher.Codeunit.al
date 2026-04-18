codeunit 99002 "PW Task Dispatcher"
{
    Access = Internal;
    TableNo = "PW Batch Chunk";

    trigger OnRun()
    var
        ErrorText: Text;
        ErrorCallStack: Text;
    begin
        if not Rec.Get(Rec."Batch Id", Rec."Chunk Index") then
            exit;

        Rec.Status := "PW Chunk Status"::Running;
        Rec."Session Id" := SessionId();
        Rec."Started At" := CurrentDateTime();
        Rec.Modify();
        // Must commit Running status before Codeunit.Run.
        // Codeunit.Run rolls back on worker failure — without this commit,
        // the Running status would be lost and we couldn't distinguish
        // "never started" from "started and failed".
        Commit();

        // Codeunit.Run rolls back all database changes on failure.
        // This is safer than TryFunction, which does NOT roll back.
        // If the worker accidentally modifies records and then errors,
        // Codeunit.Run ensures those partial writes are reverted.
        ClearLastError();
        if Codeunit.Run(Codeunit::"PW Worker Runner", Rec) then begin
            // Worker succeeded — results already saved by PW Worker Runner.
            // Re-read to pick up changes made inside Codeunit.Run.
            Rec.Get(Rec."Batch Id", Rec."Chunk Index");
            Rec.Status := "PW Chunk Status"::Completed;
            Rec."Completed At" := CurrentDateTime();
            Rec.Modify();
            // Persist chunk's Completed status before updating batch counters.
            // If UpdateBatchCounters fails (e.g. lock timeout on PW Batch),
            // the chunk is still correctly marked as Completed.
            // The next finishing chunk will recompute counters correctly.
            Commit();
        end else begin
            // Worker failed — Codeunit.Run rolled back all DB changes.
            // Re-read to ensure Rec is clean, consistent with the success branch.
            Rec.Get(Rec."Batch Id", Rec."Chunk Index");
            ErrorText := GetLastErrorText();
            ErrorCallStack := GetLastErrorCallStack();
            Rec."Error Message" := CopyStr(ErrorText, 1, MaxStrLen(Rec."Error Message"));
            WriteFullErrorMessage(Rec, ErrorText);
            WriteErrorCallStack(Rec, ErrorCallStack);
            Rec.Status := "PW Chunk Status"::Failed;
            Rec."Completed At" := CurrentDateTime();
            Rec.Modify();
            // Persist Failed status before attempting batch counter update.
            Commit();
        end;

        UpdateBatchCounters(Rec."Batch Id");
    end;

    local procedure WriteFullErrorMessage(var Chunk: Record "PW Batch Chunk"; ErrorText: Text)
    var
        OutStream: OutStream;
    begin
        Chunk."Full Error Message".CreateOutStream(OutStream, TextEncoding::UTF8);
        OutStream.WriteText(ErrorText);
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
        // UpdLock on the Batch row serializes concurrent counter updates
        // without disabling Tri-State Locking for the entire table.
        Batch.ReadIsolation := IsolationLevel::UpdLock;
        Batch.Get(BatchId);

        // ReadCommitted ensures we see committed chunk statuses only.
        // Each dispatcher commits its chunk status before calling this procedure.
        Chunk.ReadIsolation := IsolationLevel::ReadCommitted;
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
            else
                if CompletedCount = 0 then
                    Batch.Status := "PW Batch Status"::Failed
                else
                    Batch.Status := "PW Batch Status"::PartialFailure;
        end;

        Batch.Modify();
        // Release the UpdLock acquired above.
        // Without this commit, the lock is held until the session ends,
        // blocking other chunks that are finishing concurrently.
        Commit();
    end;
}
