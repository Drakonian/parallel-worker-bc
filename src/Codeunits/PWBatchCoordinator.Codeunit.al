namespace VolodymyrDvernytskyi.ParallelWorker;

/// <summary>
/// Orchestrates parallel execution by splitting work into chunks and dispatching them to background sessions.
/// </summary>
codeunit 99000 "PW Batch Coordinator"
{
    Access = Public;

    var
        ThreadCount: Integer;
        TimeoutSeconds: Integer;
        PollIntervalMs: Integer;
        WriteTransactionErr: Label 'Cannot start a parallel batch while a write transaction is active. Commit or rollback your changes before calling RunFor* methods, otherwise they would be silently committed.';

    /// <summary>
    /// Sets the number of parallel threads (background sessions) to use.
    /// </summary>
    /// <param name="Count">The number of threads. Defaults to 4 if not set.</param>
    /// <returns>The current instance for fluent chaining.</returns>
    procedure SetThreads(Count: Integer): Codeunit "PW Batch Coordinator"
    begin
        ThreadCount := Count;
        exit(this);
    end;

    /// <summary>
    /// Sets the maximum time to wait for batch completion in WaitForCompletion.
    /// </summary>
    /// <param name="Seconds">The timeout in seconds. Zero or unset means no timeout.</param>
    /// <returns>The current instance for fluent chaining.</returns>
    procedure SetTimeout(Seconds: Integer): Codeunit "PW Batch Coordinator"
    begin
        TimeoutSeconds := Seconds;
        exit(this);
    end;

    /// <summary>
    /// Sets the polling interval used by WaitForCompletion to check batch status.
    /// </summary>
    /// <param name="Milliseconds">The interval in milliseconds. Defaults to 500 if not set.</param>
    /// <returns>The current instance for fluent chaining.</returns>
    procedure SetPollInterval(Milliseconds: Integer): Codeunit "PW Batch Coordinator"
    begin
        PollIntervalMs := Milliseconds;
        exit(this);
    end;

    /// <summary>
    /// Splits a filtered record set into chunks and executes the worker in parallel background sessions.
    /// </summary>
    /// <param name="WorkerType">The enum value identifying which worker implementation to run.</param>
    /// <param name="RecRef">A RecordRef with filters applied. Records are split evenly across threads.</param>
    /// <param name="BasePayload">Additional JSON payload merged into each chunk's input.</param>
    /// <returns>The batch ID that can be used to track progress and retrieve results.</returns>
    /// <remarks>Commits the current transaction before starting background sessions. Must not be called inside a write transaction.</remarks>
    procedure RunForRecords(WorkerType: Enum "PW Worker Type"; var RecRef: RecordRef; BasePayload: JsonObject): Guid
    var
        BatchId: Guid;
        TotalCount: Integer;
        Threads: Integer;
        ChunkSize: Integer;
        ChunkCount: Integer;
        StartIdx: Integer;
        EndIdx: Integer;
        ChunkPayload: JsonObject;
        PayloadText: Text;
        FilterView: Text;
        TableNo: Integer;
        i: Integer;
    begin
        GuardNoWriteTransaction();

        TotalCount := RecRef.Count();
        if TotalCount = 0 then
            exit;

        Threads := GetEffectiveThreadCount();
        if TotalCount < Threads then
            Threads := TotalCount;
        ChunkSize := (TotalCount + Threads - 1) div Threads;
        ChunkCount := (TotalCount + ChunkSize - 1) div ChunkSize;

        BatchId := CreateBatch(WorkerType, ChunkCount);
        BasePayload.WriteTo(PayloadText);
        FilterView := RecRef.GetView();
        TableNo := RecRef.Number();

        StartIdx := 1;
        for i := 1 to ChunkCount do begin
            EndIdx := StartIdx + ChunkSize - 1;
            if EndIdx > TotalCount then
                EndIdx := TotalCount;

            Clear(ChunkPayload);
            ChunkPayload.ReadFrom(PayloadText);
            ChunkPayload.Add('$IsRecordChunk', true);
            ChunkPayload.Add('$TableNo', TableNo);
            ChunkPayload.Add('$FilterView', FilterView);
            ChunkPayload.Add('$StartIndex', StartIdx);
            ChunkPayload.Add('$EndIndex', EndIdx);

            CreateChunk(BatchId, i, ChunkPayload);

            StartIdx := EndIdx + 1;
        end;

        // Must commit batch and chunk records before StartSession.
        // StartSession fires immediately — if this transaction rolled back,
        // the background sessions would look for chunk records that no longer exist.
        Commit();
        StartBatch(BatchId);
        exit(BatchId);
    end;

    /// <summary>
    /// Splits a list of text values into chunks and executes the worker in parallel background sessions.
    /// </summary>
    /// <param name="WorkerType">The enum value identifying which worker implementation to run.</param>
    /// <param name="Items">The list of text items to distribute across chunks.</param>
    /// <param name="BasePayload">Additional JSON payload merged into each chunk's input.</param>
    /// <returns>The batch ID that can be used to track progress and retrieve results.</returns>
    /// <remarks>Commits the current transaction before starting background sessions. Must not be called inside a write transaction.</remarks>
    procedure RunForList(WorkerType: Enum "PW Worker Type"; Items: List of [Text]; BasePayload: JsonObject): Guid
    var
        BatchId: Guid;
        Threads: Integer;
        ChunkSize: Integer;
        ChunkCount: Integer;
        StartIdx: Integer;
        EndIdx: Integer;
        ChunkPayload: JsonObject;
        ItemsArray: JsonArray;
        PayloadText: Text;
        i: Integer;
        j: Integer;
    begin
        GuardNoWriteTransaction();

        if Items.Count() = 0 then
            exit;

        Threads := GetEffectiveThreadCount();
        if Items.Count() < Threads then
            Threads := Items.Count();
        ChunkSize := (Items.Count() + Threads - 1) div Threads;
        ChunkCount := (Items.Count() + ChunkSize - 1) div ChunkSize;

        BatchId := CreateBatch(WorkerType, ChunkCount);
        BasePayload.WriteTo(PayloadText);

        StartIdx := 1;
        for i := 1 to ChunkCount do begin
            EndIdx := StartIdx + ChunkSize - 1;
            if EndIdx > Items.Count() then
                EndIdx := Items.Count();

            Clear(ChunkPayload);
            ChunkPayload.ReadFrom(PayloadText);

            Clear(ItemsArray);
            for j := StartIdx to EndIdx do
                ItemsArray.Add(Items.Get(j));
            ChunkPayload.Add('$Items', ItemsArray);

            CreateChunk(BatchId, i, ChunkPayload);

            StartIdx := EndIdx + 1;
        end;

        // Must commit batch and chunk records before StartSession.
        // StartSession fires immediately — if this transaction rolled back,
        // the background sessions would look for chunk records that no longer exist.
        Commit();
        StartBatch(BatchId);
        exit(BatchId);
    end;

    /// <summary>
    /// Runs the worker in parallel for a list of pre-built chunk payloads, one background session per chunk.
    /// </summary>
    /// <param name="WorkerType">The enum value identifying which worker implementation to run.</param>
    /// <param name="Chunks">A list of JSON objects, each representing the full input for one chunk.</param>
    /// <returns>The batch ID that can be used to track progress and retrieve results.</returns>
    /// <remarks>Commits the current transaction before starting background sessions. Must not be called inside a write transaction.</remarks>
    procedure RunForChunks(WorkerType: Enum "PW Worker Type"; Chunks: List of [JsonObject]): Guid
    var
        BatchId: Guid;
        i: Integer;
    begin
        GuardNoWriteTransaction();

        if Chunks.Count() = 0 then
            exit;

        BatchId := CreateBatch(WorkerType, Chunks.Count());
        for i := 1 to Chunks.Count() do
            CreateChunk(BatchId, i, Chunks.Get(i));

        // Must commit batch and chunk records before StartSession.
        // StartSession fires immediately — if this transaction rolled back,
        // the background sessions would look for chunk records that no longer exist.
        Commit();
        StartBatch(BatchId);
        exit(BatchId);
    end;

    /// <summary>
    /// Blocks the current session until the batch finishes or the timeout elapses.
    /// </summary>
    /// <param name="BatchId">The batch identifier returned by a RunFor* method.</param>
    /// <returns>True if the batch completed successfully; false on failure, partial failure, or timeout.</returns>
    procedure WaitForCompletion(BatchId: Guid): Boolean
    var
        Batch: Record "PW Batch";
        StartTime: DateTime;
        Interval: Integer;
    begin
        if IsNullGuid(BatchId) then
            exit(true);

        // ReadUncommitted prevents polling from being blocked by
        // the dispatcher's LockTable in UpdateBatchCounters.
        // Slightly stale data is acceptable — we retry every PollInterval.
        Batch.ReadIsolation := IsolationLevel::ReadUncommitted;

        Interval := GetEffectivePollInterval();
        StartTime := CurrentDateTime();

        repeat
            if not Batch.Get(BatchId) then
                exit(false);
            case Batch.Status of
                "PW Batch Status"::Completed:
                    exit(true);
                "PW Batch Status"::Failed,
                "PW Batch Status"::PartialFailure:
                    exit(false);
            end;
            if (TimeoutSeconds > 0) and ((CurrentDateTime() - StartTime) > TimeoutSeconds * 1000) then
                exit(false);
            Sleep(Interval);
        until false;
    end;

    /// <summary>
    /// Returns the current status of a batch.
    /// </summary>
    /// <param name="BatchId">The batch identifier.</param>
    /// <returns>The batch status enum value.</returns>
    procedure GetStatus(BatchId: Guid): Enum "PW Batch Status"
    var
        Batch: Record "PW Batch";
    begin
        Batch.ReadIsolation := IsolationLevel::ReadUncommitted;
        if not Batch.Get(BatchId) then
            exit("PW Batch Status"::Failed);
        exit(Batch.Status);
    end;

    /// <summary>
    /// Returns the number of chunks that have completed successfully so far.
    /// </summary>
    /// <param name="BatchId">The batch identifier.</param>
    /// <returns>The count of completed chunks.</returns>
    procedure GetCompletedChunks(BatchId: Guid): Integer
    var
        Batch: Record "PW Batch";
    begin
        Batch.ReadIsolation := IsolationLevel::ReadUncommitted;
        if not Batch.Get(BatchId) then
            exit(0);
        exit(Batch."Completed Chunks");
    end;

    /// <summary>
    /// Returns the total number of chunks in the batch.
    /// </summary>
    /// <param name="BatchId">The batch identifier.</param>
    /// <returns>The total chunk count.</returns>
    procedure GetTotalChunks(BatchId: Guid): Integer
    var
        Batch: Record "PW Batch";
    begin
        Batch.ReadIsolation := IsolationLevel::ReadUncommitted;
        if not Batch.Get(BatchId) then
            exit(0);
        exit(Batch."Total Chunks");
    end;

    /// <summary>
    /// Checks whether the batch has reached a terminal state (Completed, Failed, or PartialFailure).
    /// </summary>
    /// <param name="BatchId">The batch identifier.</param>
    /// <returns>True if the batch is no longer running.</returns>
    procedure IsFinished(BatchId: Guid): Boolean
    var
        Batch: Record "PW Batch";
    begin
        Batch.ReadIsolation := IsolationLevel::ReadUncommitted;
        if not Batch.Get(BatchId) then
            exit(true);
        exit(Batch.Status in ["PW Batch Status"::Completed, "PW Batch Status"::Failed, "PW Batch Status"::PartialFailure]);
    end;

    /// <summary>
    /// Collects all result JSON objects from successfully completed chunks.
    /// </summary>
    /// <param name="BatchId">The batch identifier.</param>
    /// <param name="Results">The list that will be populated with result objects from all completed chunks.</param>
    procedure GetResults(BatchId: Guid; var Results: List of [JsonObject])
    var
        Chunk: Record "PW Batch Chunk";
        InStream: InStream;
        ResultArray: JsonArray;
        Token: JsonToken;
    begin
        Clear(Results);
        Chunk.SetRange("Batch Id", BatchId);
        Chunk.SetRange(Status, "PW Chunk Status"::Completed);
        if Chunk.FindSet() then
            repeat
                Chunk.CalcFields("Result Payload");
                if Chunk."Result Payload".HasValue() then begin
                    Clear(ResultArray);
                    Chunk."Result Payload".CreateInStream(InStream, TextEncoding::UTF8);
                    ResultArray.ReadFrom(InStream);
                    foreach Token in ResultArray do
                        Results.Add(Token.AsObject());
                end;
            until Chunk.Next() = 0;
    end;

    /// <summary>
    /// Collects error messages from all failed chunks in the batch.
    /// </summary>
    /// <param name="BatchId">The batch identifier.</param>
    /// <param name="Errors">The list that will be populated with error messages from failed chunks.</param>
    procedure GetErrors(BatchId: Guid; var Errors: List of [Text])
    var
        Chunk: Record "PW Batch Chunk";
    begin
        Clear(Errors);
        Chunk.SetRange("Batch Id", BatchId);
        Chunk.SetRange(Status, "PW Chunk Status"::Failed);
        if Chunk.FindSet() then
            repeat
                if Chunk."Error Message" <> '' then
                    Errors.Add(Chunk."Error Message");
            until Chunk.Next() = 0;
    end;

    /// <summary>
    /// Collects the original input payloads from all failed chunks, useful for retry scenarios.
    /// </summary>
    /// <param name="BatchId">The batch identifier.</param>
    /// <param name="FailedInputs">The list that will be populated with input JSON objects from failed chunks.</param>
    procedure GetFailedChunkInputs(BatchId: Guid; var FailedInputs: List of [JsonObject])
    var
        Chunk: Record "PW Batch Chunk";
        InStream: InStream;
        InputJson: JsonObject;
    begin
        Clear(FailedInputs);
        Chunk.SetRange("Batch Id", BatchId);
        Chunk.SetRange(Status, "PW Chunk Status"::Failed);
        if Chunk.FindSet() then
            repeat
                Chunk.CalcFields("Input Payload");
                if Chunk."Input Payload".HasValue() then begin
                    Clear(InputJson);
                    Chunk."Input Payload".CreateInStream(InStream, TextEncoding::UTF8);
                    InputJson.ReadFrom(InStream);
                    FailedInputs.Add(InputJson);
                end;
            until Chunk.Next() = 0;
    end;

    /// <summary>
    /// Deletes all batch and chunk records for the specified batch.
    /// </summary>
    /// <param name="BatchId">The batch identifier.</param>
    procedure Cleanup(BatchId: Guid)
    var
        Batch: Record "PW Batch";
        Chunk: Record "PW Batch Chunk";
    begin
        Chunk.SetRange("Batch Id", BatchId);
        Chunk.DeleteAll();

        if Batch.Get(BatchId) then
            Batch.Delete();
    end;

    local procedure GuardNoWriteTransaction()
    begin
        if Database.IsInWriteTransaction() then
            Error(WriteTransactionErr);
    end;

    local procedure GetEffectiveThreadCount(): Integer
    begin
        if ThreadCount > 0 then
            exit(ThreadCount);
        exit(4);
    end;

    local procedure GetEffectivePollInterval(): Integer
    begin
        if PollIntervalMs > 0 then
            exit(PollIntervalMs);
        exit(500);
    end;

    local procedure CreateBatch(WorkerType: Enum "PW Worker Type"; TotalChunks: Integer): Guid
    var
        Batch: Record "PW Batch";
    begin
        Batch.Id := CreateGuid();
        Batch.Status := "PW Batch Status"::Running;
        Batch."Worker Type" := WorkerType;
        Batch."Total Chunks" := TotalChunks;
        Batch."Timeout Seconds" := TimeoutSeconds;
        Batch."Created At" := CurrentDateTime();
        Batch."Company Name" := CompanyName();
        Batch.Insert();
        exit(Batch.Id);
    end;

    local procedure CreateChunk(BatchId: Guid; ChunkIdx: Integer; Payload: JsonObject)
    var
        Chunk: Record "PW Batch Chunk";
        OutStream: OutStream;
    begin
        Chunk."Batch Id" := BatchId;
        Chunk."Chunk Index" := ChunkIdx;
        Chunk.Status := "PW Chunk Status"::Pending;
        Chunk.Insert();

        Chunk."Input Payload".CreateOutStream(OutStream, TextEncoding::UTF8);
        Payload.WriteTo(OutStream);
        Chunk.Modify();
    end;

    local procedure StartBatch(BatchId: Guid)
    var
        Batch: Record "PW Batch";
        Chunk: Record "PW Batch Chunk";
        SessionId: Integer;
        FailedToStart: Integer;
    begin
        Batch.Get(BatchId);

        Chunk.SetRange("Batch Id", BatchId);
        if Chunk.FindSet() then
            repeat
                if not StartSession(SessionId, Codeunit::"PW Task Dispatcher", Batch."Company Name", Chunk) then begin
                    Chunk."Error Message" := CopyStr(GetLastErrorText(), 1, MaxStrLen(Chunk."Error Message"));
                    Chunk.Status := "PW Chunk Status"::Failed;
                    Chunk."Completed At" := CurrentDateTime();
                    Chunk.Modify();
                    FailedToStart += 1;
                end;
            until Chunk.Next() = 0;

        if FailedToStart > 0 then begin
            Batch.Get(BatchId);
            Batch."Failed Chunks" := FailedToStart;
            if FailedToStart = Batch."Total Chunks" then begin
                Batch.Status := "PW Batch Status"::Failed;
                Batch."Completed At" := CurrentDateTime();
            end;
            Batch.Modify();
            // Must commit failed-to-start chunk statuses and updated batch counters.
            // Without this, exclusive locks on the Batch row block background sessions
            // that call UpdateBatchCounters with UpdLock.
            Commit();
        end;
    end;
}
