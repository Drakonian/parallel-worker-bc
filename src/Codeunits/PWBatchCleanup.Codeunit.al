namespace VolodymyrDvernytskyi.ParallelWorker;

codeunit 99003 "PW Batch Cleanup"
{
    Access = Public;

    trigger OnRun()
    begin
        CleanupOlderThan(24);
    end;

    procedure CleanupOlderThan(Hours: Integer)
    var
        Batch: Record "PW Batch";
        Chunk: Record "PW Batch Chunk";
    begin
        Batch.SetFilter(Status, '%1|%2|%3',
            "PW Batch Status"::Completed,
            "PW Batch Status"::PartialFailure,
            "PW Batch Status"::Failed);
        Batch.SetFilter("Completed At", '<>%1', 0DT);

        if Batch.FindSet() then
            repeat
                if (CurrentDateTime() - Batch."Completed At") > Hours * 3600000 then begin
                    Chunk.SetRange("Batch Id", Batch.Id);
                    Chunk.DeleteAll();
                    Batch.Delete();
                end;
            until Batch.Next() = 0;
    end;
}
