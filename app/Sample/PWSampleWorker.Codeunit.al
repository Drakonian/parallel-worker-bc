namespace VolodymyrDvernytskyi.ParallelWorker;

codeunit 99100 "PW Sample Worker" implements "PW IParallel Worker"
{
    Access = Internal;

    procedure Execute(var Ctx: Codeunit "PW Chunk Context")
    var
        Items: JsonArray;
        Result: JsonObject;
        WorkDurationMs: Integer;
        i: Integer;
    begin
        Items := Ctx.GetItems();
        WorkDurationMs := Ctx.GetIntInput('WorkDurationMs');

        // Simulate heavy work for each item in Chunk context (subset)
        for i := 1 to Items.Count() do
            Sleep(WorkDurationMs);

        Result.Add('ChunkIndex', Ctx.GetChunkIndex());
        Result.Add('ItemsProcessed', Items.Count());
        Ctx.SetResult(Result);
    end;
}
