/// <summary>
/// Demonstrates how to orchestrate parallel work using PW Batch Coordinator.
/// Each procedure is a self-contained example of a different RunFor* pattern.
/// </summary>
codeunit 99102 "PW Sample Runner"
{
    Access = Internal;

    var
        RunnerBatchTimeoutSec: Integer;
        RunnerSessionTimeoutMs: Integer;
        TableNoRecordsLbl: Label 'Table %1 has no records.', Comment = '%1 = table number';
        CompletedRecordsLbl: Label 'Completed — %1 records across %2 chunks', Comment = '%1 = total record count, %2 = chunk count';
        CompletedTablesLbl: Label 'Completed — %1 tables counted in parallel', Comment = '%1 = number of tables';
        ProgressLbl: Label '%1 — %2 / %3 chunks completed', Comment = '%1 = batch status, %2 = completed chunks, %3 = total chunks';
        ChunkPrefixLbl: Label 'Chunk %1: ', Comment = '%1 = chunk index';
        ItemsProcessedLbl: Label '%1 items processed', Comment = '%1 = number of items';
        RecordCountLbl: Label '%1 records', Comment = '%1 = number of records';
        TotalCountedLbl: Label 'Total counted: %1', Comment = '%1 = total record count';
        TableRecordCountLbl: Label ': %1 records', Comment = '%1 = record count';

    procedure SetBatchTimeout(Seconds: Integer)
    begin
        RunnerBatchTimeoutSec := Seconds;
    end;

    procedure SetSessionTimeout(Milliseconds: Integer)
    begin
        RunnerSessionTimeoutMs := Milliseconds;
    end;

    #region RunForList — automatic list splitting

    /// <summary>
    /// RunForList example: distributes a list of work items across threads.
    /// Each item simulates work by sleeping for WorkDurationMs.
    /// </summary>
    procedure RunListSimulation(TaskCount: Integer; WorkDurationMs: Integer; ThreadCount: Integer;
        var StatusText: Text; var ExpectedMs: BigInteger; var ElapsedMs: BigInteger;
        var Speedup: Decimal; var ChunkDetails: Text)
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        Items: List of [Text];
        Payload: JsonObject;
        Results: List of [JsonObject];
        Errors: List of [Text];
        BatchId: Guid;
        StartTime: DateTime;
        i: Integer;
    begin
        for i := 1 to TaskCount do
            Items.Add(Format(i));

        Payload.Add('WorkDurationMs', WorkDurationMs);
        ExpectedMs := TaskCount * WorkDurationMs;

        StartTime := CurrentDateTime();
        BatchId := Coordinator
            .SetThreads(ThreadCount)
            .SetBatchTimeout(RunnerBatchTimeoutSec)
            .SetSessionTimeout(RunnerSessionTimeoutMs)
            .RunForList("PW Worker Type"::Sample, Items, Payload);

        if IsNullGuid(BatchId) then begin
            StatusText := 'Nothing to process.';
            exit;
        end;

        if Coordinator.WaitForCompletion(BatchId) then begin
            ElapsedMs := CurrentDateTime() - StartTime;
            StatusText := 'Completed';
            if ElapsedMs > 0 then
                Speedup := ExpectedMs / ElapsedMs;

            Coordinator.GetResults(BatchId, Results);
            ChunkDetails := FormatListResults(Results);
        end else begin
            ElapsedMs := CurrentDateTime() - StartTime;
            StatusText := Format(Coordinator.GetStatus(BatchId));

            Coordinator.GetErrors(BatchId, Errors);
            ChunkDetails := FormatErrors(Errors);
        end;

        Coordinator.Cleanup(BatchId);
    end;

    #endregion

    #region RunForRecords — automatic record splitting

    /// <summary>
    /// RunForRecords example: splits a table's records across threads and counts them.
    /// Demonstrates GetRecordRef + GetChunkSize worker pattern.
    /// </summary>
    procedure RunRecordCount(TableNo: Integer; ThreadCount: Integer;
        var StatusText: Text; var ElapsedMs: BigInteger; var ChunkDetails: Text)
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        RecRef: RecordRef;
        Payload: JsonObject;
        Results: List of [JsonObject];
        Errors: List of [Text];
        BatchId: Guid;
        StartTime: DateTime;
        TotalCount: Integer;
    begin
        RecRef.Open(TableNo);
        TotalCount := RecRef.Count();

        StartTime := CurrentDateTime();
        BatchId := Coordinator
            .SetThreads(ThreadCount)
            .SetBatchTimeout(RunnerBatchTimeoutSec)
            .SetSessionTimeout(RunnerSessionTimeoutMs)
            .RunForRecords("PW Worker Type"::RecordCounter, RecRef, Payload);

        if IsNullGuid(BatchId) then begin
            StatusText := StrSubstNo(TableNoRecordsLbl, TableNo);
            exit;
        end;

        if Coordinator.WaitForCompletion(BatchId) then begin
            ElapsedMs := CurrentDateTime() - StartTime;
            StatusText := StrSubstNo(CompletedRecordsLbl,
                TotalCount, Coordinator.GetTotalChunks(BatchId));

            Coordinator.GetResults(BatchId, Results);
            ChunkDetails := FormatRecordCountResults(Results);
        end else begin
            ElapsedMs := CurrentDateTime() - StartTime;
            StatusText := Format(Coordinator.GetStatus(BatchId));

            Coordinator.GetErrors(BatchId, Errors);
            ChunkDetails := FormatErrors(Errors);
        end;

        Coordinator.Cleanup(BatchId);
    end;

    #endregion

    #region RunForChunks — manual chunk control

    /// <summary>
    /// RunForChunks example: counts records in multiple tables simultaneously.
    /// Each chunk gets a custom payload with a different TableNo.
    /// Demonstrates manual chunk building — the caller controls what each chunk receives.
    /// </summary>
    procedure RunMultiTableCount(TableNos: List of [Integer]; var StatusText: Text;
        var ElapsedMs: BigInteger; var ChunkDetails: Text)
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        Chunks: List of [JsonObject];
        Chunk: JsonObject;
        Results: List of [JsonObject];
        Errors: List of [Text];
        BatchId: Guid;
        StartTime: DateTime;
        i: Integer;
    begin
        for i := 1 to TableNos.Count() do begin
            Clear(Chunk);
            Chunk.Add('TableNo', TableNos.Get(i));
            Chunks.Add(Chunk);
        end;

        StartTime := CurrentDateTime();
        BatchId := Coordinator
            .SetBatchTimeout(RunnerBatchTimeoutSec)
            .SetSessionTimeout(RunnerSessionTimeoutMs)
            .RunForChunks("PW Worker Type"::TableCounter, Chunks);

        if IsNullGuid(BatchId) then begin
            StatusText := 'No tables specified.';
            exit;
        end;

        if Coordinator.WaitForCompletion(BatchId) then begin
            ElapsedMs := CurrentDateTime() - StartTime;
            StatusText := StrSubstNo(CompletedTablesLbl,
                Coordinator.GetTotalChunks(BatchId));

            Coordinator.GetResults(BatchId, Results);
            ChunkDetails := FormatMultiTableResults(Results);
        end else begin
            ElapsedMs := CurrentDateTime() - StartTime;
            StatusText := Format(Coordinator.GetStatus(BatchId));

            Coordinator.GetErrors(BatchId, Errors);
            ChunkDetails := FormatErrors(Errors);
        end;

        Coordinator.Cleanup(BatchId);
    end;

    /// <summary>
    /// Parses a comma-separated string of table numbers into a list.
    /// </summary>
    procedure ParseTableNos(TableNosText: Text; var TableNos: List of [Integer])
    var
        Parts: List of [Text];
        Part: Text;
        TableNo: Integer;
    begin
        Clear(TableNos);
        Parts := TableNosText.Split(',');
        foreach Part in Parts do
            if Evaluate(TableNo, Part.Trim()) then
                TableNos.Add(TableNo);
    end;

    #endregion

    #region Non-blocking pattern — start, poll, collect

    /// <summary>
    /// Starts a batch and returns immediately — no WaitForCompletion.
    /// The caller stores the returned BatchId and checks progress later.
    /// </summary>
    procedure StartListSimulation(TaskCount: Integer; WorkDurationMs: Integer; ThreadCount: Integer): Guid
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        Items: List of [Text];
        Payload: JsonObject;
        i: Integer;
    begin
        for i := 1 to TaskCount do
            Items.Add(Format(i));

        Payload.Add('WorkDurationMs', WorkDurationMs);

        exit(Coordinator
            .SetThreads(ThreadCount)
            .RunForList("PW Worker Type"::Sample, Items, Payload));
    end;

    /// <summary>
    /// Returns a formatted progress string for a running batch.
    /// </summary>
    procedure FormatProgress(BatchId: Guid): Text
    var
        Coordinator: Codeunit "PW Batch Coordinator";
    begin
        if IsNullGuid(BatchId) then
            exit('No batch started.');

        exit(StrSubstNo(ProgressLbl,
            Coordinator.GetStatus(BatchId),
            Coordinator.GetCompletedChunks(BatchId),
            Coordinator.GetTotalChunks(BatchId)));
    end;

    /// <summary>
    /// Collects results from a finished batch and cleans up.
    /// Returns false if the batch is still running.
    /// </summary>
    procedure CollectAndCleanup(BatchId: Guid; var StatusText: Text; var ChunkDetails: Text): Boolean
    var
        Coordinator: Codeunit "PW Batch Coordinator";
        Results: List of [JsonObject];
        Errors: List of [Text];
    begin
        if IsNullGuid(BatchId) then
            exit(false);

        if not Coordinator.IsFinished(BatchId) then begin
            StatusText := 'Batch is still running.';
            exit(false);
        end;

        if Coordinator.GetStatus(BatchId) = "PW Batch Status"::Completed then begin
            StatusText := 'Completed';
            Coordinator.GetResults(BatchId, Results);
            ChunkDetails := FormatListResults(Results);
        end else begin
            StatusText := Format(Coordinator.GetStatus(BatchId));
            Coordinator.GetErrors(BatchId, Errors);
            ChunkDetails := FormatErrors(Errors);
        end;

        Coordinator.Cleanup(BatchId);
        exit(true);
    end;

    #endregion

    #region Result formatting helpers

    local procedure FormatListResults(Results: List of [JsonObject]): Text
    var
        ResultObj: JsonObject;
        Token: JsonToken;
        Builder: TextBuilder;
        i: Integer;
    begin
        for i := 1 to Results.Count() do begin
            ResultObj := Results.Get(i);
            ResultObj.Get('ChunkIndex', Token);
            Builder.Append(StrSubstNo(ChunkPrefixLbl, Token.AsValue().AsInteger()));
            ResultObj.Get('ItemsProcessed', Token);
            Builder.AppendLine(StrSubstNo(ItemsProcessedLbl, Token.AsValue().AsInteger()));
        end;
        exit(Builder.ToText());
    end;

    local procedure FormatRecordCountResults(Results: List of [JsonObject]): Text
    var
        ResultObj: JsonObject;
        Token: JsonToken;
        Builder: TextBuilder;
        TotalCounted: Integer;
        RecordCount: Integer;
        i: Integer;
    begin
        for i := 1 to Results.Count() do begin
            ResultObj := Results.Get(i);
            ResultObj.Get('ChunkIndex', Token);
            Builder.Append(StrSubstNo(ChunkPrefixLbl, Token.AsValue().AsInteger()));
            ResultObj.Get('RecordCount', Token);
            RecordCount := Token.AsValue().AsInteger();
            Builder.AppendLine(StrSubstNo(RecordCountLbl, RecordCount));
            TotalCounted += RecordCount;
        end;
        Builder.AppendLine(StrSubstNo(TotalCountedLbl, TotalCounted));
        exit(Builder.ToText());
    end;

    local procedure FormatMultiTableResults(Results: List of [JsonObject]): Text
    var
        ResultObj: JsonObject;
        Token: JsonToken;
        Builder: TextBuilder;
        i: Integer;
    begin
        for i := 1 to Results.Count() do begin
            ResultObj := Results.Get(i);
            ResultObj.Get('TableName', Token);
            Builder.Append(Token.AsValue().AsText());
            ResultObj.Get('RecordCount', Token);
            Builder.AppendLine(StrSubstNo(TableRecordCountLbl, Token.AsValue().AsInteger()));
        end;
        exit(Builder.ToText());
    end;

    local procedure FormatErrors(Errors: List of [Text]): Text
    var
        ErrorText: Text;
        Builder: TextBuilder;
    begin
        foreach ErrorText in Errors do
            Builder.AppendLine(ErrorText);
        exit(Builder.ToText());
    end;

    #endregion
}
