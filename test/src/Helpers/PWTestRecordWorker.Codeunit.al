codeunit 99208 "PW Test Record Worker" implements "PW IParallel Worker"
{
    Access = Internal;

    procedure Execute(var Ctx: Codeunit "PW Chunk Context")
    var
        RecRef: RecordRef;
        ChunkSize: Integer;
        Count: Integer;
        Result: JsonObject;
    begin
        Ctx.GetRecordRef(RecRef);
        ChunkSize := Ctx.GetChunkSize();

        repeat
            Count += 1;
        until (RecRef.Next() = 0) or (Count >= ChunkSize);

        Result.Add('ChunkIndex', Ctx.GetChunkIndex());
        Result.Add('RecordCount', Count);
        Ctx.SetResult(Result);
    end;
}
