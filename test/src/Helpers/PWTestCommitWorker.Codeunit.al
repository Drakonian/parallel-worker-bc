namespace VolodymyrDvernytskyi.ParallelWorker.Test;

using VolodymyrDvernytskyi.ParallelWorker;

codeunit 99207 "PW Test Commit Worker" implements "PW IParallel Worker"
{
    Access = Internal;

    procedure Execute(var Ctx: Codeunit "PW Chunk Context")
    var
        Result: JsonObject;
    begin
        Commit();
        Result.Add('ShouldNotReach', true);
        Ctx.SetResult(Result);
    end;
}
