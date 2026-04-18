/// <summary>
/// Defines the contract for a parallel worker that processes a single chunk of work.
/// </summary>
interface "PW IParallel Worker"
{
    /// <summary>
    /// Executes the worker logic for a single chunk of a parallel batch.
    /// </summary>
    /// <param name="ChunkContext">The context for the current chunk, providing access to input data and result storage.</param>
    procedure Execute(var ChunkContext: Codeunit "PW Chunk Context");
}
