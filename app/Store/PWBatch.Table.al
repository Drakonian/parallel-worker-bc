namespace VolodymyrDvernytskyi.ParallelWorker;

table 99000 "PW Batch"
{
    Caption = 'PW Batch';
    DataClassification = SystemMetadata;
    Extensible = false;

    fields
    {
        field(1; Id; Guid)
        {
            Caption = 'Id';
            DataClassification = SystemMetadata;
        }
        field(2; Status; Enum "PW Batch Status")
        {
            Caption = 'Status';
            DataClassification = SystemMetadata;
        }
        field(3; "Worker Type"; Enum "PW Worker Type")
        {
            Caption = 'Worker Type';
            DataClassification = SystemMetadata;
        }
        field(4; "Total Chunks"; Integer)
        {
            Caption = 'Total Chunks';
            DataClassification = SystemMetadata;
        }
        field(5; "Completed Chunks"; Integer)
        {
            Caption = 'Completed Chunks';
            DataClassification = SystemMetadata;
        }
        field(6; "Failed Chunks"; Integer)
        {
            Caption = 'Failed Chunks';
            DataClassification = SystemMetadata;
        }
        field(7; "Timeout Seconds"; Integer)
        {
            Caption = 'Timeout Seconds';
            DataClassification = SystemMetadata;
        }
        field(8; "Created At"; DateTime)
        {
            Caption = 'Created At';
            DataClassification = SystemMetadata;
        }
        field(9; "Completed At"; DateTime)
        {
            Caption = 'Completed At';
            DataClassification = SystemMetadata;
        }
        field(10; "Company Name"; Text[150])
        {
            Caption = 'Company Name';
            DataClassification = SystemMetadata;
        }
    }

    keys
    {
        key(PK; Id)
        {
            Clustered = true;
        }
    }
}
