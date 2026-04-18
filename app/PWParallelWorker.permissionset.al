permissionset 99000 "PW Parallel Worker"
{
    Assignable = true;
    Permissions = tabledata "PW Batch" = RIMD,
        tabledata "PW Batch Chunk" = RIMD,
        table "PW Batch" = X,
        table "PW Batch Chunk" = X,
        codeunit "PW Batch Cleanup" = X,
        codeunit "PW Batch Coordinator" = X,
        codeunit "PW Chunk Context" = X,
        codeunit "PW Sample Record Counter" = X,
        codeunit "PW Sample Runner" = X,
        codeunit "PW Sample Table Counter" = X,
        codeunit "PW Sample Worker" = X,
        codeunit "PW Task Dispatcher" = X,
        codeunit "PW Worker Runner" = X,
        page "PW Batch Chunk List" = X,
        page "PW Batches" = X,
        page "PW Demo" = X;
}