namespace VolodymyrDvernytskyi.ParallelWorker;

interface "PW IParallel Worker"
{
    procedure Execute(var ChunkContext: Codeunit "PW Chunk Context");
}
