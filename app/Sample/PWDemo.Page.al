page 99100 "PW Demo"
{
    ApplicationArea = All;
    Caption = 'PW Parallel Worker Demo';
    PageType = Card;
    UsageCategory = Administration;

    layout
    {
        area(Content)
        {
            group(Input)
            {
                Caption = 'Input';

                field(TaskCount; TaskCount)
                {
                    Caption = 'Task Count';
                    ToolTip = 'Number of work items. Used by Run Simulation and Non-Blocking demos.';
                    MinValue = 1;
                }
                field(WorkDurationMs; WorkDurationMs)
                {
                    Caption = 'Work Duration (ms)';
                    ToolTip = 'Simulated work time per item in milliseconds.';
                    MinValue = 50;
                }
                field(TableNo; TableNo)
                {
                    Caption = 'Table No.';
                    ToolTip = 'Table to count records from. Used by Run Record Count.';
                    MinValue = 1;
                }
                field(TableNosText; TableNosText)
                {
                    Caption = 'Table Numbers';
                    ToolTip = 'Comma-separated table numbers. Used by Run Multi-Table Count.';
                }
                field(ThreadCount; ThreadCount)
                {
                    Caption = 'Thread Count';
                    ToolTip = 'Number of parallel background sessions.';
                    MinValue = 1;
                    MaxValue = 10;
                }
                field(BatchTimeoutSec; BatchTimeoutSec)
                {
                    Caption = 'Batch Timeout (s)';
                    ToolTip = 'Max time the caller waits for batch completion. 0 = wait forever.';
                    MinValue = 0;
                }
                field(SessionTimeoutMs; SessionTimeoutMs)
                {
                    Caption = 'Session Timeout (ms)';
                    ToolTip = 'Max runtime per session. Platform kills the session if exceeded. 0 = no limit.';
                    MinValue = 0;
                }
            }
            group(Output)
            {
                Caption = 'Results';

                field(StatusText; StatusText)
                {
                    Caption = 'Status';
                    ToolTip = 'Final status of the batch.';
                    Editable = false;
                }
                field(ExpectedSequentialMs; ExpectedSequentialMs)
                {
                    Caption = 'Expected Sequential (ms)';
                    ToolTip = 'How long this would take in a single thread.';
                    Editable = false;
                }
                field(ElapsedMs; ElapsedMs)
                {
                    Caption = 'Actual Parallel (ms)';
                    ToolTip = 'How long the parallel execution actually took.';
                    Editable = false;
                }
                field(Speedup; Speedup)
                {
                    Caption = 'Speedup';
                    ToolTip = 'Expected sequential time divided by actual parallel time.';
                    Editable = false;
                    DecimalPlaces = 1 : 1;
                }
                field(BatchIdText; BatchIdText)
                {
                    Caption = 'Batch ID';
                    ToolTip = 'Non-blocking: the running batch ID.';
                    Editable = false;
                }
                field(ProgressText; ProgressText)
                {
                    Caption = 'Progress';
                    ToolTip = 'Non-blocking: click Refresh to update.';
                    Editable = false;
                }
            }
            group(Details)
            {
                Caption = 'Chunk Details';

                field(ChunkDetails; ChunkDetails)
                {
                    Caption = 'Details';
                    ToolTip = 'Per-chunk processing details.';
                    Editable = false;
                    MultiLine = true;
                }
            }
        }
    }

    actions
    {
        area(Promoted)
        {
            group(BlockingDemos)
            {
                Caption = 'Blocking';
                actionref(RunSimulationRef; RunSimulation) { }
                actionref(RunRecordCountRef; RunRecordCount) { }
                actionref(RunMultiTableRef; RunMultiTable) { }
            }
            group(NonBlockingDemos)
            {
                Caption = 'Non-Blocking';
                actionref(StartRef; StartNonBlocking) { }
                actionref(RefreshRef; RefreshProgress) { }
                actionref(CollectRef; CollectAndCleanup) { }
            }
        }
        area(Processing)
        {
            action(RunSimulation)
            {
                Caption = 'Run Simulation';
                ToolTip = 'RunForList pattern: split items, simulate work, measure speedup.';
                Image = Start;

                trigger OnAction()
                begin
                    ClearOutput();
                    SampleRunner.SetBatchTimeout(BatchTimeoutSec);
                    SampleRunner.SetSessionTimeout(SessionTimeoutMs);
                    SampleRunner.RunListSimulation(
                        TaskCount, WorkDurationMs, ThreadCount,
                        StatusText, ExpectedSequentialMs, ElapsedMs, Speedup, ChunkDetails);
                end;
            }
            action(RunRecordCount)
            {
                Caption = 'Run Record Count';
                ToolTip = 'RunForRecords pattern: split table records, count per chunk.';
                Image = Calculate;

                trigger OnAction()
                begin
                    ClearOutput();
                    SampleRunner.SetBatchTimeout(BatchTimeoutSec);
                    SampleRunner.SetSessionTimeout(SessionTimeoutMs);
                    SampleRunner.RunRecordCount(
                        TableNo, ThreadCount,
                        StatusText, ElapsedMs, ChunkDetails);
                end;
            }
            action(RunMultiTable)
            {
                Caption = 'Run Multi-Table Count';
                ToolTip = 'RunForChunks pattern: one chunk per table, count each in parallel.';
                Image = Splitlines;

                trigger OnAction()
                var
                    TableNos: List of [Integer];
                begin
                    ClearOutput();
                    SampleRunner.SetBatchTimeout(BatchTimeoutSec);
                    SampleRunner.SetSessionTimeout(SessionTimeoutMs);
                    SampleRunner.ParseTableNos(TableNosText, TableNos);
                    SampleRunner.RunMultiTableCount(
                        TableNos,
                        StatusText, ElapsedMs, ChunkDetails);
                end;
            }
            action(StartNonBlocking)
            {
                Caption = '1. Start';
                ToolTip = 'Fire-and-forget: start batch, return immediately.';
                Image = Start;

                trigger OnAction()
                begin
                    ClearOutput();
                    NonBlockingBatchId := SampleRunner.StartListSimulation(
                        TaskCount, WorkDurationMs, ThreadCount);

                    if IsNullGuid(NonBlockingBatchId) then begin
                        ProgressText := 'Nothing to process.';
                        exit;
                    end;

                    BatchIdText := Format(NonBlockingBatchId);
                    ProgressText := 'Started — click Refresh to check progress.';
                end;
            }
            action(RefreshProgress)
            {
                Caption = '2. Refresh';
                ToolTip = 'Poll batch status without blocking.';
                Image = Refresh;

                trigger OnAction()
                begin
                    ProgressText := SampleRunner.FormatProgress(NonBlockingBatchId);
                end;
            }
            action(CollectAndCleanup)
            {
                Caption = '3. Collect';
                ToolTip = 'Collect results (if finished) and delete batch records.';
                Image = GetLines;

                trigger OnAction()
                begin
                    if SampleRunner.CollectAndCleanup(NonBlockingBatchId, StatusText, ChunkDetails) then begin
                        ProgressText := 'Done — batch cleaned up.';
                        Clear(NonBlockingBatchId);
                        Clear(BatchIdText);
                    end;
                end;
            }
        }
    }

    var
        SampleRunner: Codeunit "PW Sample Runner";
        NonBlockingBatchId: Guid;
        TaskCount: Integer;
        WorkDurationMs: Integer;
        ThreadCount: Integer;
        BatchTimeoutSec: Integer;
        SessionTimeoutMs: Integer;
        TableNo: Integer;
        TableNosText: Text;
        StatusText: Text;
        ExpectedSequentialMs: BigInteger;
        ElapsedMs: BigInteger;
        Speedup: Decimal;
        BatchIdText: Text;
        ProgressText: Text;
        ChunkDetails: Text;

    trigger OnOpenPage()
    begin
        TaskCount := 20;
        WorkDurationMs := 200;
        ThreadCount := 4;
        BatchTimeoutSec := 300;
        SessionTimeoutMs := 60000;
        TableNo := 17;
        TableNosText := '8,9,18,17,27';
    end;

    local procedure ClearOutput()
    begin
        Clear(StatusText);
        Clear(ExpectedSequentialMs);
        Clear(ElapsedMs);
        Clear(Speedup);
        Clear(BatchIdText);
        Clear(ProgressText);
        Clear(ChunkDetails);
    end;
}
