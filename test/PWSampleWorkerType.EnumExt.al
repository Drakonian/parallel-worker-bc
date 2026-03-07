namespace VolodymyrDvernytskyi.ParallelWorker;

enumextension 99100 "PW Sample Worker Type" extends "PW Worker Type"
{
    value(99100; SampleCount)
    {
        Caption = 'Sample Count';
        Implementation = "PW IParallel Worker" = "PW Sample Count Worker";
    }
}
