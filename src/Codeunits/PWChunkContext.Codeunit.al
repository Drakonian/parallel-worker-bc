namespace VolodymyrDvernytskyi.ParallelWorker;

codeunit 99001 "PW Chunk Context"
{
    Access = Public;

    var
        InputJson: JsonObject;
        Results: JsonArray;
        BatchId: Guid;
        ChunkIndex: Integer;
        IsInitialized: Boolean;
        KeyNotFoundErr: Label 'Key "%1" not found in chunk input.';
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

    procedure GetBatchId(): Guid
    begin
        CheckInitialized();
        exit(BatchId);
    end;

    procedure GetChunkIndex(): Integer
    begin
        CheckInitialized();
        exit(ChunkIndex);
    end;

    procedure GetInput(): JsonObject
    begin
        CheckInitialized();
        exit(InputJson);
    end;

    procedure GetTextInput(InputKey: Text): Text
    var
        Token: JsonToken;
    begin
        CheckInitialized();
        if not InputJson.Get(InputKey, Token) then
            Error(KeyNotFoundErr, InputKey);
        exit(Token.AsValue().AsText());
    end;

    procedure GetIntInput(InputKey: Text): Integer
    var
        Token: JsonToken;
    begin
        CheckInitialized();
        if not InputJson.Get(InputKey, Token) then
            Error(KeyNotFoundErr, InputKey);
        exit(Token.AsValue().AsInteger());
    end;

    procedure GetDecimalInput(InputKey: Text): Decimal
    var
        Token: JsonToken;
    begin
        CheckInitialized();
        if not InputJson.Get(InputKey, Token) then
            Error(KeyNotFoundErr, InputKey);
        exit(Token.AsValue().AsDecimal());
    end;

    procedure GetBoolInput(InputKey: Text): Boolean
    var
        Token: JsonToken;
    begin
        CheckInitialized();
        if not InputJson.Get(InputKey, Token) then
            Error(KeyNotFoundErr, InputKey);
        exit(Token.AsValue().AsBoolean());
    end;

    procedure GetInputArray(InputKey: Text): JsonArray
    var
        Token: JsonToken;
    begin
        CheckInitialized();
        if not InputJson.Get(InputKey, Token) then
            Error(KeyNotFoundErr, InputKey);
        exit(Token.AsArray());
    end;

    procedure IsRecordChunk(): Boolean
    begin
        CheckInitialized();
        exit(InputJson.Contains('$IsRecordChunk'));
    end;

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

    procedure GetEndIndex(): Integer
    begin
        exit(GetIntInput('$EndIndex') - GetIntInput('$StartIndex') + 1);
    end;

    procedure SetResult(Result: JsonObject)
    begin
        Clear(Results);
        Results.Add(Result);
    end;

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
