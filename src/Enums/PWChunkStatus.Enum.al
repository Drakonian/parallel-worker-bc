namespace VolodymyrDvernytskyi.ParallelWorker;

enum 99001 "PW Chunk Status"
{
    Extensible = false;

    value(0; Pending)
    {
        Caption = 'Pending';
    }
    value(1; Running)
    {
        Caption = 'Running';
    }
    value(2; Completed)
    {
        Caption = 'Completed';
    }
    value(3; Failed)
    {
        Caption = 'Failed';
    }
}
