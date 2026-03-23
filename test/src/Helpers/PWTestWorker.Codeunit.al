namespace VolodymyrDvernytskyi.ParallelWorker.Test;

using VolodymyrDvernytskyi.ParallelWorker;

codeunit 99205 "PW Test Worker" implements "PW IParallel Worker"
{
    Access = Internal;

    procedure Execute(var Ctx: Codeunit "PW Chunk Context")
    var
        Input: JsonObject;
        Token: JsonToken;
        Result: JsonObject;
    begin
        Input := Ctx.GetInput();

        if Input.Get('$ShouldFail', Token) then
            if Token.AsValue().AsBoolean() then
                Error('Test error: chunk configured to fail');

        if Ctx.HasItems() then
            Result.Add('ItemCount', Ctx.GetItems().Count());

        Result.Add('ChunkIndex', Ctx.GetChunkIndex());
        Ctx.SetResult(Result);
    end;
}
