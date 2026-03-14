namespace VolodymyrDvernytskyi.ParallelWorker;

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
            group(ListSimulation)
            {
                Caption = 'List Simulation (RunForList)';

                field(TaskCount; TaskCount)
                {
                    Caption = 'Task Count';
                    ToolTip = 'Number of work items to process.';
                    MinValue = 1;
                }
                field(WorkDurationMs; WorkDurationMs)
                {
                    Caption = 'Work Duration (ms)';
                    ToolTip = 'Simulated work time per item in milliseconds.';
                    MinValue = 50;
                }
            }
            group(RecordCountGroup)
            {
                Caption = 'Record Count (RunForRecords)';

                field(TableNo; TableNo)
                {
                    Caption = 'Table No.';
                    ToolTip = 'Table to count records from in parallel.';
                    MinValue = 1;
                }
            }
            group(MultiTableGroup)
            {
                Caption = 'Multi-Table Count (RunForChunks)';

                field(TableNosText; TableNosText)
                {
                    Caption = 'Table Numbers';
                    ToolTip = 'Comma-separated table numbers to count in parallel (e.g. 8,9,18,27).';
                }
            }
            group(Settings)
            {
                Caption = 'Settings';

                field(ThreadCount; ThreadCount)
                {
                    Caption = 'Thread Count';
                    ToolTip = 'Number of parallel threads to use.';
                    MinValue = 1;
                    MaxValue = 10;
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
            }
            group(NonBlocking)
            {
                Caption = 'Non-Blocking (fire-and-forget)';

                field(BatchIdText; BatchIdText)
                {
                    Caption = 'Batch ID';
                    ToolTip = 'The ID of the running batch. Stored for later polling.';
                    Editable = false;
                }
                field(ProgressText; ProgressText)
                {
                    Caption = 'Progress';
                    ToolTip = 'Current progress — click Refresh to update.';
                    Editable = false;
                }
            }
            group(ChunkResults)
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
                ToolTip = 'Run the parallel list simulation (RunForList pattern).';
                Image = Start;

                trigger OnAction()
                begin
                    ClearOutput();
                    SampleRunner.RunListSimulation(
                        TaskCount, WorkDurationMs, ThreadCount,
                        StatusText, ExpectedSequentialMs, ElapsedMs, Speedup, ChunkDetails);
                end;
            }
            action(RunRecordCount)
            {
                Caption = 'Run Record Count';
                ToolTip = 'Count records in a table using parallel sessions (RunForRecords pattern).';
                Image = Calculate;

                trigger OnAction()
                begin
                    ClearOutput();
                    SampleRunner.RunRecordCount(
                        TableNo, ThreadCount,
                        StatusText, ElapsedMs, ChunkDetails);
                end;
            }
            action(RunMultiTable)
            {
                Caption = 'Run Multi-Table Count';
                ToolTip = 'Count records in multiple tables simultaneously (RunForChunks pattern).';
                Image = Splitlines;

                trigger OnAction()
                var
                    TableNos: List of [Integer];
                begin
                    ClearOutput();
                    SampleRunner.ParseTableNos(TableNosText, TableNos);
                    SampleRunner.RunMultiTableCount(
                        TableNos,
                        StatusText, ElapsedMs, ChunkDetails);
                end;
            }
            action(StartNonBlocking)
            {
                Caption = '1. Start';
                ToolTip = 'Start a slow batch and return immediately — no waiting.';
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
                ToolTip = 'Poll the batch status without blocking.';
                Image = Refresh;

                trigger OnAction()
                begin
                    ProgressText := SampleRunner.FormatProgress(NonBlockingBatchId);
                end;
            }
            action(CollectAndCleanup)
            {
                Caption = '3. Collect';
                ToolTip = 'Collect results (if finished) and delete the batch records.';
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
