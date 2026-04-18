enum 99000 "PW Batch Status"
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
    value(3; PartialFailure)
    {
        Caption = 'Partial Failure';
    }
    value(4; Failed)
    {
        Caption = 'Failed';
    }
}
