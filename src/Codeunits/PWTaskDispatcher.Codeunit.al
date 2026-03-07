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
        Commit();

        ChunkContext.Init(Rec);

        ClearLastError();
        if TryExecuteWorker(Batch."Worker Type", ChunkContext) then begin
            ChunkContext.SaveResultsTo(Rec);
            Rec.Status := "PW Chunk Status"::Completed;
            Rec."Completed At" := CurrentDateTime();
            Rec.Modify();
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
            Commit();
        end;

        UpdateBatchCounters(Rec."Batch Id");
    end;

    [TryFunction]
    local procedure TryExecuteWorker(WorkerType: Enum "PW Worker Type"; var Context: Codeunit "PW Chunk Context")
    var
        Worker: Interface "PW IParallel Worker";
    begin
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
        Commit();
    end;
}
