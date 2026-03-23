namespace VolodymyrDvernytskyi.ParallelWorker;

/// <summary>
/// Sample worker that counts records in a chunk range.
/// Demonstrates the RunForRecords pattern: GetRecordRef + GetChunkSize iteration.
/// </summary>
codeunit 99101 "PW Sample Record Counter" implements "PW IParallel Worker"
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
            Count += 1; //your slow process with Chunk of records
        until (RecRef.Next() = 0) or (Count >= ChunkSize);

        Result.Add('ChunkIndex', Ctx.GetChunkIndex());
        Result.Add('RecordCount', Count);
        Ctx.SetResult(Result);
    end;
}
