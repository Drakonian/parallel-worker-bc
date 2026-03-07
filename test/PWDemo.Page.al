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

                field(TableNo; TableNo)
                {
                    Caption = 'Table No.';
                    ToolTip = 'The table to count records from.';
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
                    Caption = 'Batch Status';
                    ToolTip = 'Final status of the batch.';
                    Editable = false;
                }
                field(TotalRecords; TotalRecords)
                {
                    Caption = 'Total Records Counted';
                    ToolTip = 'Sum of records counted across all chunks.';
                    Editable = false;
                }
                field(ElapsedMs; ElapsedMs)
                {
                    Caption = 'Elapsed (ms)';
                    ToolTip = 'Time taken for the batch to complete.';
                    Editable = false;
                }
            }
            group(ChunkResults)
            {
                Caption = 'Chunk Details';

                field(ChunkDetails; ChunkDetails)
                {
                    Caption = 'Details';
                    ToolTip = 'Per-chunk record counts.';
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
            actionref(RunParallelRef; RunParallel) { }
        }
        area(Processing)
        {
            action(RunParallel)
            {
                Caption = 'Run Parallel Count';
                ToolTip = 'Count all records in the specified table using parallel workers.';
                Image = Start;

                trigger OnAction()
                var
                    Coordinator: Codeunit "PW Batch Coordinator";
                    RecRef: RecordRef;
                    Results: List of [JsonObject];
                    Errors: List of [Text];
                    ResultObj: JsonObject;
                    Token: JsonToken;
                    BatchId: Guid;
                    StartTime: DateTime;
                    ChunkIdx: Integer;
                    ChunkCount: Integer;
                    DetailsBuilder: TextBuilder;
                    ErrorText: Text;
                begin
                    if TableNo = 0 then
                        Error('Please enter a Table No.');

                    Clear(StatusText);
                    Clear(TotalRecords);
                    Clear(ElapsedMs);
                    Clear(ChunkDetails);

                    RecRef.Open(TableNo);
                    if RecRef.IsEmpty() then begin
                        StatusText := 'Table is empty.';
                        exit;
                    end;

                    StartTime := CurrentDateTime();
                    BatchId := Coordinator
                        .SetThreads(ThreadCount)
                        .SetTimeout(120)
                        .RunForRecords("PW Worker Type"::SampleCount, RecRef, EmptyPayload());

                    if IsNullGuid(BatchId) then begin
                        StatusText := 'Nothing to process.';
                        exit;
                    end;

                    if Coordinator.WaitForCompletion(BatchId) then begin
                        StatusText := 'Completed';
                        ElapsedMs := CurrentDateTime() - StartTime;

                        Coordinator.GetResults(BatchId, Results);
                        foreach ResultObj in Results do begin
                            ResultObj.Get('ChunkIndex', Token);
                            ChunkIdx := Token.AsValue().AsInteger();
                            ResultObj.Get('RecordCount', Token);
                            ChunkCount := Token.AsValue().AsInteger();
                            TotalRecords += ChunkCount;
                            DetailsBuilder.AppendLine(StrSubstNo('Chunk %1: %2 records', ChunkIdx, ChunkCount));
                        end;
                        ChunkDetails := DetailsBuilder.ToText();
                    end else begin
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
        TableNo: Integer;
        ThreadCount: Integer;
        StatusText: Text;
        TotalRecords: Integer;
        ElapsedMs: BigInteger;
        ChunkDetails: Text;

    trigger OnOpenPage()
    begin
        ThreadCount := 4;
    end;

    local procedure EmptyPayload(): JsonObject
    var
        Payload: JsonObject;
    begin
        exit(Payload);
    end;
}
