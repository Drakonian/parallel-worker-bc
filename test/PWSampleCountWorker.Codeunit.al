namespace VolodymyrDvernytskyi.ParallelWorker;

codeunit 99100 "PW Sample Count Worker" implements "PW IParallel Worker"
{
    Access = Internal;

    procedure Execute(var Ctx: Codeunit "PW Chunk Context")
    var
        RecRef: RecordRef;
        RecordCount: Integer;
        ChunkSize: Integer;
        Result: JsonObject;
    begin
        Ctx.GetRecordRef(RecRef);
        ChunkSize := Ctx.GetEndIndex();

        repeat
            RecordCount += 1;
        until (RecRef.Next() = 0) or (RecordCount >= ChunkSize);

        Result.Add('ChunkIndex', Ctx.GetChunkIndex());
        Result.Add('RecordCount', RecordCount);
        Ctx.SetResult(Result);
    end;
}
