namespace VolodymyrDvernytskyi.ParallelWorker.Test;

using VolodymyrDvernytskyi.ParallelWorker;

enumextension 99201 "PW Test Worker Type" extends "PW Worker Type"
{
    value(99201; TestWorker)
    {
        Caption = 'Test Worker';
        Implementation = "PW IParallel Worker" = "PW Test Worker";
    }
    value(99202; TestErrorWorker)
    {
        Caption = 'Test Error Worker';
        Implementation = "PW IParallel Worker" = "PW Test Error Worker";
    }
    value(99203; TestCommitWorker)
    {
        Caption = 'Test Commit Worker';
        Implementation = "PW IParallel Worker" = "PW Test Commit Worker";
    }
    value(99204; TestRecordWorker)
    {
        Caption = 'Test Record Worker';
        Implementation = "PW IParallel Worker" = "PW Test Record Worker";
    }
}
