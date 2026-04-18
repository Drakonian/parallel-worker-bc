codeunit 99204 "PW Test Helper"
{
    Access = Internal;

    procedure CreateBatch(Status: Enum "PW Batch Status"): Guid
    var
        Batch: Record "PW Batch";
    begin
        Batch.Id := CreateGuid();
        Batch.Status := Status;
        Batch."Created At" := CurrentDateTime();
        Batch."Company Name" := CompanyName();
        Batch.Insert();
        exit(Batch.Id);
    end;

    procedure CreateBatchEx(Status: Enum "PW Batch Status"; TotalChunks: Integer; CompletedChunks: Integer; FailedChunks: Integer): Guid
    var
        Batch: Record "PW Batch";
    begin
        Batch.Id := CreateGuid();
        Batch.Status := Status;
        Batch."Total Chunks" := TotalChunks;
        Batch."Completed Chunks" := CompletedChunks;
        Batch."Failed Chunks" := FailedChunks;
        Batch."Created At" := CurrentDateTime();
        Batch."Company Name" := CompanyName();
        Batch.Insert();
        exit(Batch.Id);
    end;

    procedure CreateBatchWithTimestamp(Status: Enum "PW Batch Status"; CompletedAt: DateTime): Guid
    var
        Batch: Record "PW Batch";
    begin
        Batch.Id := CreateGuid();
        Batch.Status := Status;
        Batch."Created At" := CurrentDateTime();
        Batch."Completed At" := CompletedAt;
        Batch."Company Name" := CompanyName();
        Batch.Insert();
        exit(Batch.Id);
    end;

    procedure CreateChunkWithPayload(BatchId: Guid; ChunkIndex: Integer; Payload: JsonObject)
    var
        Chunk: Record "PW Batch Chunk";
        OutStream: OutStream;
    begin
        Chunk."Batch Id" := BatchId;
        Chunk."Chunk Index" := ChunkIndex;
        Chunk.Status := "PW Chunk Status"::Pending;
        Chunk.Insert();

        Chunk."Input Payload".CreateOutStream(OutStream, TextEncoding::UTF8);
        Payload.WriteTo(OutStream);
        Chunk.Modify();
    end;

    procedure CreateChunkWithResult(BatchId: Guid; ChunkIndex: Integer; ResultArray: JsonArray)
    var
        Chunk: Record "PW Batch Chunk";
        OutStream: OutStream;
    begin
        Chunk."Batch Id" := BatchId;
        Chunk."Chunk Index" := ChunkIndex;
        Chunk.Status := "PW Chunk Status"::Completed;
        Chunk.Insert();

        Chunk."Result Payload".CreateOutStream(OutStream, TextEncoding::UTF8);
        ResultArray.WriteTo(OutStream);
        Chunk.Modify();
    end;

    procedure CreateChunkWithError(BatchId: Guid; ChunkIndex: Integer; ErrorMsg: Text; InputPayload: JsonObject)
    var
        Chunk: Record "PW Batch Chunk";
        OutStream: OutStream;
    begin
        Chunk."Batch Id" := BatchId;
        Chunk."Chunk Index" := ChunkIndex;
        Chunk.Status := "PW Chunk Status"::Failed;
        Chunk."Error Message" := CopyStr(ErrorMsg, 1, MaxStrLen(Chunk."Error Message"));
        Chunk.Insert();

        Chunk."Input Payload".CreateOutStream(OutStream, TextEncoding::UTF8);
        InputPayload.WriteTo(OutStream);
        Chunk.Modify();
    end;

    procedure CreateSimpleChunk(BatchId: Guid; ChunkIndex: Integer; Status: Enum "PW Chunk Status")
    var
        Chunk: Record "PW Batch Chunk";
    begin
        Chunk."Batch Id" := BatchId;
        Chunk."Chunk Index" := ChunkIndex;
        Chunk.Status := Status;
        Chunk.Insert();
    end;
}
