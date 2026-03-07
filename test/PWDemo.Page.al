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
            group(Input)
            {
                Caption = 'Input';

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
            actionref(RunRef; Run) { }
        }
        area(Processing)
        {
            action(Run)
            {
                Caption = 'Run';
                ToolTip = 'Run the parallel work simulation.';
                Image = Start;

                trigger OnAction()
                var
                    Coordinator: Codeunit "PW Batch Coordinator";
                    Items: List of [Text];
                    Payload: JsonObject;
                    Results: List of [JsonObject];
                    Errors: List of [Text];
                    ResultObj: JsonObject;
                    Token: JsonToken;
                    BatchId: Guid;
                    StartTime: DateTime;
                    ChunkIdx: Integer;
                    ItemsProcessed: Integer;
                    DetailsBuilder: TextBuilder;
                    ErrorText: Text;
                    i: Integer;
                begin
                    Clear(StatusText);
                    Clear(ElapsedMs);
                    Clear(Speedup);
                    Clear(ChunkDetails);

                    ExpectedSequentialMs := TaskCount * WorkDurationMs;

                    for i := 1 to TaskCount do
                        Items.Add(Format(i));

                    Payload.Add('WorkDurationMs', WorkDurationMs);

                    StartTime := CurrentDateTime();
                    BatchId := Coordinator
                        .SetThreads(ThreadCount)
                        .SetTimeout(300)
                        .RunForList("PW Worker Type"::Sample, Items, Payload);

                    if IsNullGuid(BatchId) then begin
                        StatusText := 'Nothing to process.';
                        exit;
                    end;

                    if Coordinator.WaitForCompletion(BatchId) then begin
                        ElapsedMs := CurrentDateTime() - StartTime;
                        StatusText := 'Completed';
                        if ElapsedMs > 0 then
                            Speedup := ExpectedSequentialMs / ElapsedMs;

                        Coordinator.GetResults(BatchId, Results);
                        foreach ResultObj in Results do begin
                            ResultObj.Get('ChunkIndex', Token);
                            ChunkIdx := Token.AsValue().AsInteger();
                            ResultObj.Get('ItemsProcessed', Token);
                            ItemsProcessed := Token.AsValue().AsInteger();
                            DetailsBuilder.AppendLine(
                                StrSubstNo('Chunk %1: %2 items', ChunkIdx, ItemsProcessed));
                        end;
                        ChunkDetails := DetailsBuilder.ToText();
                    end else begin
                        ElapsedMs := CurrentDateTime() - StartTime;
                        StatusText := Format(Coordinator.GetStatus(BatchId));

                        Coordinator.GetErrors(BatchId, Errors);
                        foreach ErrorText in Errors do
                            DetailsBuilder.AppendLine(ErrorText);
                        ChunkDetails := DetailsBuilder.ToText();
                    end;

                    Coordinator.Cleanup(BatchId);
                end;
            }
        }
    }

    var
        TaskCount: Integer;
        WorkDurationMs: Integer;
        ThreadCount: Integer;
        StatusText: Text;
        ExpectedSequentialMs: BigInteger;
        ElapsedMs: BigInteger;
        Speedup: Decimal;
        ChunkDetails: Text;

    trigger OnOpenPage()
    begin
        TaskCount := 20;
        WorkDurationMs := 200;
        ThreadCount := 4;
    end;
}
