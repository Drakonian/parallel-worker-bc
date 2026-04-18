/// <summary>
/// Sample worker for RunForChunks pattern.
/// Each chunk receives a TableNo in its payload and counts all records in that table.
/// Unlike PW Sample Record Counter (which uses GetRecordRef for RunForRecords),
/// this worker reads custom payload keys — demonstrating manual chunk control.
/// </summary>
codeunit 99103 "PW Sample Table Counter" implements "PW IParallel Worker"
{
    Access = Internal;

    procedure Execute(var Ctx: Codeunit "PW Chunk Context")
    var
        RecRef: RecordRef;
        TableNo: Integer;
        Result: JsonObject;
    begin
        TableNo := Ctx.GetIntInput('TableNo');
        RecRef.Open(TableNo);

        Result.Add('ChunkIndex', Ctx.GetChunkIndex());
        Result.Add('TableNo', TableNo);
        Result.Add('TableName', RecRef.Name());
        Result.Add('RecordCount', RecRef.Count());
        Ctx.SetResult(Result);
    end;
}
