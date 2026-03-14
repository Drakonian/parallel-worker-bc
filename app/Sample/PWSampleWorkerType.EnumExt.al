namespace VolodymyrDvernytskyi.ParallelWorker;

enumextension 99100 "PW Sample Worker Type" extends "PW Worker Type"
{
    value(99100; Sample)
    {
        Caption = 'Sample';
        Implementation = "PW IParallel Worker" = "PW Sample Worker";
    }
    value(99101; RecordCounter)
    {
        Caption = 'Record Counter';
        Implementation = "PW IParallel Worker" = "PW Sample Record Counter";
    }
    value(99102; TableCounter)
    {
        Caption = 'Table Counter';
        Implementation = "PW IParallel Worker" = "PW Sample Table Counter";
    }
}
