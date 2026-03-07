namespace VolodymyrDvernytskyi.ParallelWorker;

enumextension 99100 "PW Sample Worker Type" extends "PW Worker Type"
{
    value(99100; Sample)
    {
        Caption = 'Sample';
        Implementation = "PW IParallel Worker" = "PW Sample Worker";
    }
}
