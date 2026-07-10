/// <summary>
/// Orchestrates parallel execution by splitting work into chunks and dispatching them to background sessions.
/// </summary>
codeunit 99000 "PW Batch Coordinator"
{
    Access = Public;

    var
        ThreadCount: Integer;
        BatchTimeoutSeconds: Integer;
        SessionTimeoutMs: Integer;
        PollIntervalMs: Integer;
        NextDeadSessionCheckAt: DateTime;
        StalledBatchSeen: Boolean;
        WriteTransactionErr: Label 'Cannot start a parallel batch while a write transaction is active. Commit or rollback your changes before calling RunFor* methods, otherwise they would be silently committed.';
        WaitWriteTransactionErr: Label 'Cannot wait for a parallel batch while a write transaction is active. WaitForCompletion may commit internally during recovery, which would silently commit your changes.';
        SessionTerminatedErr: Label 'Session terminated (timeout or server stop).';
        InvalidThreadCountErr: Label 'Thread count must be at least 1.';
        ReservedKeyErr: Label 'The payload key %1 is not allowed. Keys prefixed with $ are reserved for the framework.', Comment = '%1 = JSON key name';

    /// <summary>
    /// Sets the number of parallel threads (background sessions) to use.
    /// </summary>
    /// <param name="Count">The number of threads. Must be at least 1. Defaults to 4 if not set.</param>
    /// <returns>The current instance for fluent chaining.</returns>
    procedure SetThreads(Count: Integer): Codeunit "PW Batch Coordinator"
    begin
        if Count < 1 then
            Error(InvalidThreadCountErr);
        ThreadCount := Count;
        exit(this);
    end;

    /// <summary>
    /// Sets the maximum time the caller waits in WaitForCompletion before giving up.
    /// This is a client-side timeout — background sessions keep running after it expires.
    /// Use SetSessionTimeout to enforce a hard kill on individual sessions.
    /// </summary>
    /// <param name="Seconds">The timeout in seconds. Zero or unset means wait indefinitely.</param>
    /// <returns>The current instance for fluent chaining.</returns>
    procedure SetBatchTimeout(Seconds: Integer): Codeunit "PW Batch Coordinator"
    begin
        BatchTimeoutSeconds := Seconds;
        exit(this);
    end;

    /// <summary>
    /// Sets the maximum time each background session is allowed to run.
    /// If a session exceeds this duration, the platform terminates it and the chunk is marked as Failed.
    /// This is a server-side timeout — protects against hung or runaway workers.
    /// </summary>
    /// <param name="Milliseconds">The timeout in milliseconds. Zero or unset leaves the platform's default background session timeout in charge.</param>
    /// <returns>The current instance for fluent chaining.</returns>
    procedure SetSessionTimeout(Milliseconds: Integer): Codeunit "PW Batch Coordinator"
    begin
        SessionTimeoutMs := Milliseconds;
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
    /// <returns>The batch ID that can be used to track progress and retrieve results.</returns>
    /// <remarks>Commits the current transaction before starting background sessions. Must not be called inside a write transaction.</remarks>
    procedure RunForRecords(WorkerType: Enum "PW Worker Type"; var RecRef: RecordRef): Guid
    var
        EmptyPayload: JsonObject;
    begin
        exit(RunForRecords(WorkerType, RecRef, EmptyPayload));
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
        AssertNoReservedKeys(BasePayload);

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
    /// <returns>The batch ID that can be used to track progress and retrieve results.</returns>
    /// <remarks>Commits the current transaction before starting background sessions. Must not be called inside a write transaction.</remarks>
    procedure RunForList(WorkerType: Enum "PW Worker Type"; Items: List of [Text]): Guid
    var
        EmptyPayload: JsonObject;
    begin
        exit(RunForList(WorkerType, Items, EmptyPayload));
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
        AssertNoReservedKeys(BasePayload);

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
    /// <remarks>Must not be called while a write transaction is active — recovery paths commit internally.</remarks>
    procedure WaitForCompletion(BatchId: Guid): Boolean
    var
        Batch: Record "PW Batch";
        StartTime: DateTime;
        Interval: Integer;
    begin
        if IsNullGuid(BatchId) then
            exit(true);

        // Recovery below commits internally, which would silently persist any
        // pending caller changes — same rule as the RunFor* methods.
        if Database.IsInWriteTransaction() then
            Error(WaitWriteTransactionErr);

        // ReadUncommitted prevents polling from being blocked by
        // the dispatcher's UpdLock in UpdateBatchCounters.
        // Slightly stale data is acceptable — we retry every PollInterval.
        Batch.ReadIsolation := IsolationLevel::ReadUncommitted;

        Interval := GetEffectivePollInterval();
        StartTime := CurrentDateTime();
        NextDeadSessionCheckAt := 0DT;
        StalledBatchSeen := false;

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

            // If session timeout is configured and enough time has passed,
            // check for sessions killed by the platform (hard kill leaves chunks in Running).
            if ShouldCheckDeadSessions(StartTime) then
                RecoverDeadSessions(BatchId);

            ReconcileStalledBatch(BatchId);

            if (BatchTimeoutSeconds > 0) and ((CurrentDateTime() - StartTime) > BatchTimeoutSeconds * 1000) then
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
    /// Reads the full error from the Blob field when available, falls back to the Text[2048] field.
    /// </summary>
    /// <param name="BatchId">The batch identifier.</param>
    /// <param name="Errors">The list that will be populated with error messages from failed chunks.</param>
    procedure GetErrors(BatchId: Guid; var Errors: List of [Text])
    var
        Chunk: Record "PW Batch Chunk";
        InStream: InStream;
    begin
        Clear(Errors);
        Chunk.SetAutoCalcFields("Full Error Message");
        Chunk.SetRange("Batch Id", BatchId);
        Chunk.SetRange(Status, "PW Chunk Status"::Failed);
        if Chunk.FindSet() then
            repeat
                if Chunk."Full Error Message".HasValue() then begin
                    Chunk."Full Error Message".CreateInStream(InStream, TextEncoding::UTF8);
                    Errors.Add(ReadBlobText(InStream));
                end else
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
    /// Convenience method: runs a list of items in parallel, waits for completion,
    /// and collects results — all in one call. Batch records are kept for monitoring.
    /// </summary>
    procedure RunAndWaitForList(WorkerType: Enum "PW Worker Type"; Items: List of [Text]; var Results: List of [JsonObject]): Boolean
    var
        EmptyPayload: JsonObject;
        DiscardErrors: List of [Text];
    begin
        exit(RunAndWaitForList(WorkerType, Items, EmptyPayload, false, Results, DiscardErrors));
    end;

    /// <summary>
    /// Convenience method: runs a list of items in parallel, waits for completion,
    /// and collects results — all in one call. Batch records are kept for monitoring.
    /// </summary>
    procedure RunAndWaitForList(WorkerType: Enum "PW Worker Type"; Items: List of [Text]; BasePayload: JsonObject; var Results: List of [JsonObject]): Boolean
    var
        DiscardErrors: List of [Text];
    begin
        exit(RunAndWaitForList(WorkerType, Items, BasePayload, false, Results, DiscardErrors));
    end;

    /// <summary>
    /// Convenience method: runs a list of items in parallel, waits for completion,
    /// and collects results — all in one call.
    /// </summary>
    procedure RunAndWaitForList(WorkerType: Enum "PW Worker Type"; Items: List of [Text]; BasePayload: JsonObject; AutoCleanup: Boolean; var Results: List of [JsonObject]): Boolean
    var
        DiscardErrors: List of [Text];
    begin
        exit(RunAndWaitForList(WorkerType, Items, BasePayload, AutoCleanup, Results, DiscardErrors));
    end;

    /// <summary>
    /// Convenience method: runs a list of items in parallel, waits for completion,
    /// collects results and errors — all in one call.
    /// </summary>
    /// <param name="WorkerType">The enum value identifying which worker implementation to run.</param>
    /// <param name="Items">The list of text items to distribute across chunks.</param>
    /// <param name="BasePayload">Additional JSON payload merged into each chunk's input.</param>
    /// <param name="AutoCleanup">True to delete batch and chunk records after completion; false to keep them.</param>
    /// <param name="Results">Populated with result objects from completed chunks.</param>
    /// <param name="Errors">Populated with error messages from failed chunks.</param>
    /// <returns>True if all chunks completed successfully; false on failure or partial failure.</returns>
    procedure RunAndWaitForList(WorkerType: Enum "PW Worker Type"; Items: List of [Text]; BasePayload: JsonObject; AutoCleanup: Boolean; var Results: List of [JsonObject]; var Errors: List of [Text]): Boolean
    var
        BatchId: Guid;
        Success: Boolean;
    begin
        BatchId := RunForList(WorkerType, Items, BasePayload);
        if IsNullGuid(BatchId) then begin
            Clear(Results);
            Clear(Errors);
            exit(true);
        end;

        Success := WaitForCompletion(BatchId);
        GetResults(BatchId, Results);
        GetErrors(BatchId, Errors);

        if AutoCleanup then
            Cleanup(BatchId);
        exit(Success);
    end;

    /// <summary>
    /// Convenience method: splits a filtered record set across threads, waits for completion,
    /// and collects results — all in one call. Batch records are kept for monitoring.
    /// </summary>
    procedure RunAndWaitForRecords(WorkerType: Enum "PW Worker Type"; var RecRef: RecordRef; var Results: List of [JsonObject]): Boolean
    var
        EmptyPayload: JsonObject;
        DiscardErrors: List of [Text];
    begin
        exit(RunAndWaitForRecords(WorkerType, RecRef, EmptyPayload, false, Results, DiscardErrors));
    end;

    /// <summary>
    /// Convenience method: splits a filtered record set across threads, waits for completion,
    /// and collects results — all in one call. Batch records are kept for monitoring.
    /// </summary>
    procedure RunAndWaitForRecords(WorkerType: Enum "PW Worker Type"; var RecRef: RecordRef; BasePayload: JsonObject; var Results: List of [JsonObject]): Boolean
    var
        DiscardErrors: List of [Text];
    begin
        exit(RunAndWaitForRecords(WorkerType, RecRef, BasePayload, false, Results, DiscardErrors));
    end;

    /// <summary>
    /// Convenience method: splits a filtered record set across threads, waits for completion,
    /// and collects results — all in one call.
    /// </summary>
    procedure RunAndWaitForRecords(WorkerType: Enum "PW Worker Type"; var RecRef: RecordRef; BasePayload: JsonObject; AutoCleanup: Boolean; var Results: List of [JsonObject]): Boolean
    var
        DiscardErrors: List of [Text];
    begin
        exit(RunAndWaitForRecords(WorkerType, RecRef, BasePayload, AutoCleanup, Results, DiscardErrors));
    end;

    /// <summary>
    /// Convenience method: splits a filtered record set across threads, waits for completion,
    /// collects results and errors — all in one call.
    /// </summary>
    /// <param name="WorkerType">The enum value identifying which worker implementation to run.</param>
    /// <param name="RecRef">A RecordRef with filters applied. Records are split evenly across threads.</param>
    /// <param name="BasePayload">Additional JSON payload merged into each chunk's input.</param>
    /// <param name="AutoCleanup">True to delete batch and chunk records after completion; false to keep them.</param>
    /// <param name="Results">Populated with result objects from completed chunks.</param>
    /// <param name="Errors">Populated with error messages from failed chunks.</param>
    /// <returns>True if all chunks completed successfully; false on failure or partial failure.</returns>
    procedure RunAndWaitForRecords(WorkerType: Enum "PW Worker Type"; var RecRef: RecordRef; BasePayload: JsonObject; AutoCleanup: Boolean; var Results: List of [JsonObject]; var Errors: List of [Text]): Boolean
    var
        BatchId: Guid;
        Success: Boolean;
    begin
        BatchId := RunForRecords(WorkerType, RecRef, BasePayload);
        if IsNullGuid(BatchId) then begin
            Clear(Results);
            Clear(Errors);
            exit(true);
        end;

        Success := WaitForCompletion(BatchId);
        GetResults(BatchId, Results);
        GetErrors(BatchId, Errors);

        if AutoCleanup then
            Cleanup(BatchId);
        exit(Success);
    end;

    /// <summary>
    /// Convenience method: runs pre-built chunks in parallel, waits for completion,
    /// and collects results — all in one call. Batch records are kept for monitoring.
    /// </summary>
    procedure RunAndWaitForChunks(WorkerType: Enum "PW Worker Type"; Chunks: List of [JsonObject]; var Results: List of [JsonObject]): Boolean
    var
        DiscardErrors: List of [Text];
    begin
        exit(RunAndWaitForChunks(WorkerType, Chunks, false, Results, DiscardErrors));
    end;

    /// <summary>
    /// Convenience method: runs pre-built chunks in parallel, waits for completion,
    /// and collects results — all in one call.
    /// </summary>
    procedure RunAndWaitForChunks(WorkerType: Enum "PW Worker Type"; Chunks: List of [JsonObject]; AutoCleanup: Boolean; var Results: List of [JsonObject]): Boolean
    var
        DiscardErrors: List of [Text];
    begin
        exit(RunAndWaitForChunks(WorkerType, Chunks, AutoCleanup, Results, DiscardErrors));
    end;

    /// <summary>
    /// Convenience method: runs pre-built chunks in parallel, waits for completion,
    /// collects results and errors — all in one call.
    /// </summary>
    /// <param name="WorkerType">The enum value identifying which worker implementation to run.</param>
    /// <param name="Chunks">A list of JSON objects, each representing the full input for one chunk.</param>
    /// <param name="AutoCleanup">True to delete batch and chunk records after completion; false to keep them.</param>
    /// <param name="Results">Populated with result objects from completed chunks.</param>
    /// <param name="Errors">Populated with error messages from failed chunks.</param>
    /// <returns>True if all chunks completed successfully; false on failure or partial failure.</returns>
    procedure RunAndWaitForChunks(WorkerType: Enum "PW Worker Type"; Chunks: List of [JsonObject]; AutoCleanup: Boolean; var Results: List of [JsonObject]; var Errors: List of [Text]): Boolean
    var
        BatchId: Guid;
        Success: Boolean;
    begin
        BatchId := RunForChunks(WorkerType, Chunks);
        if IsNullGuid(BatchId) then begin
            Clear(Results);
            Clear(Errors);
            exit(true);
        end;

        Success := WaitForCompletion(BatchId);
        GetResults(BatchId, Results);
        GetErrors(BatchId, Errors);

        if AutoCleanup then
            Cleanup(BatchId);
        exit(Success);
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

    local procedure WriteFullErrorToChunk(var Chunk: Record "PW Batch Chunk"; ErrorText: Text)
    var
        OutStream: OutStream;
    begin
        Chunk."Full Error Message".CreateOutStream(OutStream, TextEncoding::UTF8);
        OutStream.WriteText(ErrorText);
    end;

    local procedure ReadBlobText(InStream: InStream): Text
    var
        Builder: TextBuilder;
        Line: Text;
    begin
        // ReadText stops at line breaks — loop to preserve multi-line content.
        while not InStream.EOS() do begin
            InStream.ReadText(Line);
            if Builder.Length() > 0 then
                Builder.Append(NewLineText());
            Builder.Append(Line);
        end;
        exit(Builder.ToText());
    end;

    local procedure NewLineText(): Text
    var
        LineFeed: Char;
    begin
        LineFeed := 10;
        exit(Format(LineFeed));
    end;

    local procedure AssertNoReservedKeys(BasePayload: JsonObject)
    var
        PayloadKey: Text;
    begin
        foreach PayloadKey in BasePayload.Keys() do
            if PayloadKey.StartsWith('$') then
                Error(ReservedKeyErr, PayloadKey);
    end;

    local procedure ShouldCheckDeadSessions(StartTime: DateTime): Boolean
    var
        ArmDelay: Duration;
    begin
        if SessionTimeoutMs <= 0 then
            exit(false);
        // Arm after session timeout + 2s margin for session startup overhead.
        // Duration arithmetic avoids Integer overflow near MaxInt timeouts.
        ArmDelay := SessionTimeoutMs;
        ArmDelay += 2000;
        if (CurrentDateTime() - StartTime) <= ArmDelay then
            exit(false);
        // Sessions start — and die — at different times (queued sessions,
        // staggered starts), so re-check periodically rather than once.
        exit(CurrentDateTime() >= NextDeadSessionCheckAt);
    end;

    local procedure RecoverDeadSessions(BatchId: Guid)
    var
        Chunk: Record "PW Batch Chunk";
        AnyRecovered: Boolean;
    begin
        NextDeadSessionCheckAt := CurrentDateTime() + 2000;

        Chunk.SetRange("Batch Id", BatchId);
        Chunk.SetFilter(Status, '%1|%2', "PW Chunk Status"::Pending, "PW Chunk Status"::Running);
        // Pending chunks carry the session id stored by StartBatch. A session id
        // of 0 means the session never started — the failed-to-start path in
        // StartBatch already handles those chunks.
        Chunk.SetFilter("Session Id", '<>0');
        if Chunk.FindSet() then
            repeat
                if not Session.IsSessionActive(Chunk."Session Id") then
                    if MarkDeadChunkFailed(Chunk."Batch Id", Chunk."Chunk Index") then
                        AnyRecovered := true;
            until Chunk.Next() = 0;

        if AnyRecovered then begin
            // Persist failed chunk statuses and release their row locks
            // before taking the batch UpdLock in RecountBatchCounters.
            Commit();
            RecountBatchCounters(BatchId);
        end;
    end;

    local procedure MarkDeadChunkFailed(BatchId: Guid; ChunkIdx: Integer): Boolean
    var
        Chunk: Record "PW Batch Chunk";
    begin
        // UpdLock + status re-check closes the race where the dispatcher finishes
        // the chunk between the polling snapshot and this update — a plain Modify
        // could otherwise fail the version check or overwrite a terminal status.
        Chunk.ReadIsolation := IsolationLevel::UpdLock;
        if not Chunk.Get(BatchId, ChunkIdx) then
            exit(false);
        if not (Chunk.Status in ["PW Chunk Status"::Pending, "PW Chunk Status"::Running]) then
            exit(false);

        Chunk."Error Message" := CopyStr(SessionTerminatedErr, 1, MaxStrLen(Chunk."Error Message"));
        WriteFullErrorToChunk(Chunk, SessionTerminatedErr);
        Chunk.Status := "PW Chunk Status"::Failed;
        Chunk."Completed At" := CurrentDateTime();
        Chunk.Modify();
        exit(true);
    end;

    local procedure ReconcileStalledBatch(BatchId: Guid)
    var
        Chunk: Record "PW Batch Chunk";
    begin
        // A dispatcher that dies between committing its chunk status and updating
        // the batch counters leaves the batch Running forever. If every chunk has
        // a committed terminal status while the batch still says Running, recount.
        // Requiring the condition on two consecutive polls avoids grabbing the
        // batch UpdLock while the last dispatcher is about to update it anyway.
        Chunk.ReadIsolation := IsolationLevel::ReadCommitted;
        Chunk.SetRange("Batch Id", BatchId);
        Chunk.SetFilter(Status, '%1|%2', "PW Chunk Status"::Pending, "PW Chunk Status"::Running);
        if not Chunk.IsEmpty() then begin
            StalledBatchSeen := false;
            exit;
        end;

        if not StalledBatchSeen then begin
            StalledBatchSeen := true;
            exit;
        end;

        StalledBatchSeen := false;
        RecountBatchCounters(BatchId);
    end;

    /// <summary>
    /// Recounts chunk statuses and updates batch counters with UpdLock serialization.
    /// Shared by StartBatch (failed-to-start recovery), RecoverDeadSessions and
    /// ReconcileStalledBatch. Same pattern as UpdateBatchCounters in PW Task Dispatcher.
    /// </summary>
    local procedure RecountBatchCounters(BatchId: Guid)
    var
        Batch: Record "PW Batch";
        Chunk: Record "PW Batch Chunk";
        CompletedCount: Integer;
        FailedCount: Integer;
    begin
        Batch.ReadIsolation := IsolationLevel::UpdLock;
        // The batch disappears if the caller runs Cleanup mid-batch.
        if not Batch.Get(BatchId) then
            exit;

        Chunk.ReadIsolation := IsolationLevel::ReadCommitted;
        Chunk.SetRange("Batch Id", BatchId);
        Chunk.SetRange(Status, "PW Chunk Status"::Completed);
        CompletedCount := Chunk.Count();

        Chunk.SetRange(Status, "PW Chunk Status"::Failed);
        FailedCount := Chunk.Count();

        Batch."Completed Chunks" := CompletedCount;
        Batch."Failed Chunks" := FailedCount;

        if (CompletedCount + FailedCount) = Batch."Total Chunks" then begin
            Batch."Completed At" := CurrentDateTime();
            if FailedCount = 0 then
                Batch.Status := "PW Batch Status"::Completed
            else
                if CompletedCount = 0 then
                    Batch.Status := "PW Batch Status"::Failed
                else
                    Batch.Status := "PW Batch Status"::PartialFailure;
        end;

        Batch.Modify();
        Commit();
    end;

    local procedure CreateBatch(WorkerType: Enum "PW Worker Type"; TotalChunks: Integer): Guid
    var
        Batch: Record "PW Batch";
    begin
        Batch.Id := CreateGuid();
        Batch.Status := "PW Batch Status"::Running;
        Batch."Worker Type" := WorkerType;
        Batch."Total Chunks" := TotalChunks;
        Batch."Timeout Seconds" := BatchTimeoutSeconds;
        Batch."Created At" := CurrentDateTime();
        Batch."Company Name" := CopyStr(CompanyName(), 1, MaxStrLen(Batch."Company Name"));
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
        StartedSessions: Dictionary of [Integer, Integer];
        SessionId: Integer;
        ChunkIdx: Integer;
        FailedToStart: Integer;
    begin
        Batch.Get(BatchId);

        Chunk.SetRange("Batch Id", BatchId);
        if Chunk.FindSet() then
            repeat
                if StartDispatcherSession(SessionId, Batch."Company Name", Chunk) then
                    StartedSessions.Add(Chunk."Chunk Index", SessionId)
                else begin
                    Chunk."Error Message" := CopyStr(GetLastErrorText(), 1, MaxStrLen(Chunk."Error Message"));
                    WriteFullErrorToChunk(Chunk, GetLastErrorText());
                    Chunk.Status := "PW Chunk Status"::Failed;
                    Chunk."Completed At" := CurrentDateTime();
                    Chunk.Modify();
                    FailedToStart += 1;
                end;
            until Chunk.Next() = 0;

        // Session ids are stored after all sessions are started: storing inside
        // the loop would hold each chunk's row lock for the whole StartSession
        // sequence and lock-timeout dispatchers on their first read.
        foreach ChunkIdx in StartedSessions.Keys() do
            StoreSessionId(BatchId, ChunkIdx, StartedSessions.Get(ChunkIdx));

        // Persist session ids (dead-session recovery needs them for chunks that
        // never reach Running) and failed-to-start statuses, release all row
        // locks, and leave the caller without an open write transaction —
        // RunFor* must return with a clean transaction state.
        Commit();

        if FailedToStart > 0 then
            RecountBatchCounters(BatchId);
    end;

    local procedure StartDispatcherSession(var NewSessionId: Integer; RunInCompany: Text; var Chunk: Record "PW Batch Chunk"): Boolean
    begin
        // An explicit 0 for the timeout parameter is undocumented platform
        // behavior — when no session timeout is configured, use the overload
        // that leaves the server default in charge.
        if SessionTimeoutMs > 0 then
            exit(StartSession(NewSessionId, Codeunit::"PW Task Dispatcher", RunInCompany, Chunk, SessionTimeoutMs));
        exit(StartSession(NewSessionId, Codeunit::"PW Task Dispatcher", RunInCompany, Chunk));
    end;

    local procedure StoreSessionId(BatchId: Guid; ChunkIdx: Integer; NewSessionId: Integer)
    var
        Chunk: Record "PW Batch Chunk";
    begin
        // UpdLock serializes with the dispatcher claiming this chunk. If the
        // dispatcher won the race, the chunk is no longer Pending and already
        // carries its own session id.
        Chunk.ReadIsolation := IsolationLevel::UpdLock;
        Chunk.Get(BatchId, ChunkIdx);
        if Chunk.Status <> "PW Chunk Status"::Pending then
            exit;
        Chunk."Session Id" := NewSessionId;
        Chunk.Modify();
    end;
}
