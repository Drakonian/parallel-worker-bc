codeunit 99004 "PW Worker Runner"
{
    Access = Internal;
    TableNo = "PW Batch Chunk";

    trigger OnRun()
    var
        Batch: Record "PW Batch";
        ChunkContext: Codeunit "PW Chunk Context";
    begin
        Batch.Get(Rec."Batch Id");
        ChunkContext.Init(Rec);
        RunWorkerNoCommit(Batch."Worker Type", ChunkContext);
        ChunkContext.SaveResultsTo(Rec);
        Rec.Modify();
    end;

    [CommitBehavior(CommitBehavior::Error)]
    local procedure RunWorkerNoCommit(WorkerType: Enum "PW Worker Type"; var Context: Codeunit "PW Chunk Context")
    var
        Worker: Interface "PW IParallel Worker";
    begin
        // CommitBehavior::Error prevents workers from calling explicit Commit() inside Execute.
        // If a worker calls Commit(), a runtime error is raised immediately.
        // Codeunit.Run catches it and rolls back all database changes.
        Worker := WorkerType;
        Worker.Execute(Context);
    end;
}
