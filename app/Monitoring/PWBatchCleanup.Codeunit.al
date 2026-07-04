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
        CutoffOffset: Duration;
    begin
        // Duration arithmetic — Hours * 3600000 as Integer overflows at ~596 hours.
        CutoffOffset := Hours;
        CutoffOffset := CutoffOffset * 3600000;

        Batch.SetFilter(Status, '%1|%2|%3',
            "PW Batch Status"::Completed,
            "PW Batch Status"::PartialFailure,
            "PW Batch Status"::Failed);
        Batch.SetFilter("Completed At", '<>%1&<%2', 0DT, CurrentDateTime() - CutoffOffset);
        // OnDelete on PW Batch removes the chunks.
        Batch.DeleteAll(true);
    end;
}
