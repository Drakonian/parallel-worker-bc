namespace VolodymyrDvernytskyi.ParallelWorker;

page 99000 "PW Batches"
{
    ApplicationArea = All;
    Caption = 'PW Batches';
    PageType = List;
    SourceTable = "PW Batch";
    UsageCategory = Lists;
    Editable = false;

    layout
    {
        area(Content)
        {
            repeater(Batches)
            {
                field(Id; Rec.Id)
                {
                    ToolTip = 'Unique identifier of the batch.';
                }
                field(Status; Rec.Status)
                {
                    ToolTip = 'Current status of the batch.';
                }
                field("Worker Type"; Rec."Worker Type")
                {
                    ToolTip = 'The worker implementation used for this batch.';
                }
                field("Completed Chunks"; Rec."Completed Chunks")
                {
                    ToolTip = 'Number of chunks that completed successfully.';
                }
                field("Failed Chunks"; Rec."Failed Chunks")
                {
                    ToolTip = 'Number of chunks that failed.';
                }
                field("Total Chunks"; Rec."Total Chunks")
                {
                    ToolTip = 'Total number of chunks in this batch.';
                }
                field("Created At"; Rec."Created At")
                {
                    ToolTip = 'When the batch was created.';
                }
                field("Completed At"; Rec."Completed At")
                {
                    ToolTip = 'When the last chunk finished.';
                }
            }
        }
    }

    actions
    {
        area(Promoted)
        {
            actionref(ViewChunksRef; ViewChunks) { }
            actionref(CleanupSelectedRef; CleanupSelected) { }
        }
        area(Processing)
        {
            action(ViewChunks)
            {
                Caption = 'View Chunks';
                ToolTip = 'View all chunks for this batch.';
                Image = List;
                RunObject = page "PW Batch Chunk List";
                RunPageLink = "Batch Id" = field(Id);
            }
            action(CleanupSelected)
            {
                Caption = 'Cleanup Selected';
                ToolTip = 'Delete this batch and all its chunk records.';
                Image = Delete;

                trigger OnAction()
                var
                    Coordinator: Codeunit "PW Batch Coordinator";
                begin
                    if not Confirm(DeleteBatchQst, false, Rec.Id) then
                        exit;
                    Coordinator.Cleanup(Rec.Id);
                    CurrPage.Update(false);
                end;
            }
            action(CleanupOlderThan24h)
            {
                Caption = 'Cleanup Older Than 24h';
                ToolTip = 'Delete all finished batches completed more than 24 hours ago.';
                Image = ClearLog;

                trigger OnAction()
                var
                    BatchCleanup: Codeunit "PW Batch Cleanup";
                begin
                    if not Confirm(DeleteOlderQst) then
                        exit;
                    BatchCleanup.CleanupOlderThan(24);
                    CurrPage.Update(false);
                end;
            }
        }
    }
    var
        DeleteBatchQst: Label 'Delete batch %1 and all its chunks?', Comment = '%1 = batch ID';
        DeleteOlderQst: Label 'Delete all finished batches older than 24 hours?';
}
