namespace VolodymyrDvernytskyi.ParallelWorker;

/// <summary>
/// Provides access to the input data and result storage for a single chunk during parallel execution.
/// </summary>
codeunit 99001 "PW Chunk Context"
{
    Access = Public;

    var
        InputJson: JsonObject;
        Results: JsonArray;
        BatchId: Guid;
        ChunkIndex: Integer;
        IsInitialized: Boolean;
        KeyNotFoundErr: Label 'Key "%1" not found in chunk input.', Comment = '%1 = JSON key name';
        NotRecordChunkErr: Label 'This chunk was not created via RunForRecords.';
        NotInitializedErr: Label 'Chunk context has not been initialized.';

    procedure Init(var Chunk: Record "PW Batch Chunk")
    var
        InStream: InStream;
    begin
        BatchId := Chunk."Batch Id";
        ChunkIndex := Chunk."Chunk Index";
        Clear(InputJson);
        Clear(Results);

        Chunk.CalcFields("Input Payload");
        if Chunk."Input Payload".HasValue() then begin
            Chunk."Input Payload".CreateInStream(InStream, TextEncoding::UTF8);
            InputJson.ReadFrom(InStream);
        end;

        IsInitialized := true;
    end;

    procedure SaveResultsTo(var Chunk: Record "PW Batch Chunk")
    var
        OutStream: OutStream;
    begin
        if Results.Count() = 0 then
            exit;

        Chunk."Result Payload".CreateOutStream(OutStream, TextEncoding::UTF8);
        Results.WriteTo(OutStream);
    end;

    /// <summary>
    /// Returns the unique identifier of the batch this chunk belongs to.
    /// </summary>
    /// <returns>The batch GUID.</returns>
    procedure GetBatchId(): Guid
    begin
        CheckInitialized();
        exit(BatchId);
    end;

    /// <summary>
    /// Returns the 1-based index of this chunk within the batch.
    /// </summary>
    /// <returns>The chunk index.</returns>
    procedure GetChunkIndex(): Integer
    begin
        CheckInitialized();
        exit(ChunkIndex);
    end;

    /// <summary>
    /// Returns the full input payload for this chunk as a JSON object.
    /// </summary>
    /// <returns>The chunk's input JSON object.</returns>
    procedure GetInput(): JsonObject
    begin
        CheckInitialized();
        exit(InputJson);
    end;

    /// <summary>
    /// Reads a text value from the chunk input by key.
    /// </summary>
    /// <param name="InputKey">The JSON key to look up.</param>
    /// <returns>The text value associated with the key.</returns>
    procedure GetTextInput(InputKey: Text): Text
    var
        Token: JsonToken;
    begin
        CheckInitialized();
        if not InputJson.Get(InputKey, Token) then
            Error(KeyNotFoundErr, InputKey);
        exit(Token.AsValue().AsText());
    end;

    /// <summary>
    /// Reads an integer value from the chunk input by key.
    /// </summary>
    /// <param name="InputKey">The JSON key to look up.</param>
    /// <returns>The integer value associated with the key.</returns>
    procedure GetIntInput(InputKey: Text): Integer
    var
        Token: JsonToken;
    begin
        CheckInitialized();
        if not InputJson.Get(InputKey, Token) then
            Error(KeyNotFoundErr, InputKey);
        exit(Token.AsValue().AsInteger());
    end;

    /// <summary>
    /// Reads a decimal value from the chunk input by key.
    /// </summary>
    /// <param name="InputKey">The JSON key to look up.</param>
    /// <returns>The decimal value associated with the key.</returns>
    procedure GetDecimalInput(InputKey: Text): Decimal
    var
        Token: JsonToken;
    begin
        CheckInitialized();
        if not InputJson.Get(InputKey, Token) then
            Error(KeyNotFoundErr, InputKey);
        exit(Token.AsValue().AsDecimal());
    end;

    /// <summary>
    /// Reads a boolean value from the chunk input by key.
    /// </summary>
    /// <param name="InputKey">The JSON key to look up.</param>
    /// <returns>The boolean value associated with the key.</returns>
    procedure GetBoolInput(InputKey: Text): Boolean
    var
        Token: JsonToken;
    begin
        CheckInitialized();
        if not InputJson.Get(InputKey, Token) then
            Error(KeyNotFoundErr, InputKey);
        exit(Token.AsValue().AsBoolean());
    end;

    /// <summary>
    /// Reads a JSON array from the chunk input by key.
    /// </summary>
    /// <param name="InputKey">The JSON key to look up.</param>
    /// <returns>The JSON array associated with the key.</returns>
    procedure GetInputArray(InputKey: Text): JsonArray
    var
        Token: JsonToken;
    begin
        CheckInitialized();
        if not InputJson.Get(InputKey, Token) then
            Error(KeyNotFoundErr, InputKey);
        exit(Token.AsArray());
    end;

    /// <summary>
    /// Indicates whether this chunk was created by RunForRecords and contains record-based input.
    /// </summary>
    /// <returns>True if the chunk carries record data; false otherwise.</returns>
    procedure IsRecordChunk(): Boolean
    begin
        CheckInitialized();
        exit(InputJson.Contains('$IsRecordChunk'));
    end;

    /// <summary>
    /// Opens a RecordRef positioned at the first record of this chunk's range.
    /// </summary>
    /// <param name="RecRef">The RecordRef that will be opened and positioned on the chunk's starting record.</param>
    /// <remarks>Only valid for chunks created by RunForRecords. Errors if called on a non-record chunk.</remarks>
    procedure GetRecordRef(var RecRef: RecordRef)
    var
        TableNo: Integer;
        FilterView: Text;
        StartIndex: Integer;
    begin
        if not IsRecordChunk() then
            Error(NotRecordChunkErr);

        TableNo := GetIntInput('$TableNo');
        StartIndex := GetIntInput('$StartIndex');

        RecRef.Open(TableNo);

        FilterView := GetTextInput('$FilterView');
        if FilterView <> '' then
            RecRef.SetView(FilterView);

        RecRef.FindSet();
        if StartIndex > 1 then
            RecRef.Next(StartIndex - 1);
    end;

    /// <summary>
    /// Returns the number of records this chunk should process, based on its start and end index range.
    /// </summary>
    /// <returns>The count of records in this chunk's range.</returns>
    procedure GetChunkSize(): Integer
    begin
        exit(GetIntInput('$EndIndex') - GetIntInput('$StartIndex') + 1);
    end;

    /// <summary>
    /// Sets a single JSON object as the chunk's result, replacing any previously stored results.
    /// </summary>
    /// <param name="Result">The JSON object to store as the sole result.</param>
    procedure SetResult(Result: JsonObject)
    begin
        Clear(Results);
        Results.Add(Result);
    end;

    /// <summary>
    /// Appends a JSON object to the chunk's result list without clearing existing results.
    /// </summary>
    /// <param name="Result">The JSON object to append.</param>
    procedure AppendResult(Result: JsonObject)
    begin
        Results.Add(Result);
    end;

    local procedure CheckInitialized()
    begin
        if not IsInitialized then
            Error(NotInitializedErr);
    end;
}
