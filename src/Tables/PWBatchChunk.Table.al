namespace VolodymyrDvernytskyi.ParallelWorker;

table 99001 "PW Batch Chunk"
{
    Caption = 'PW Batch Chunk';
    DataClassification = SystemMetadata;
    Extensible = false;

    fields
    {
        field(1; "Batch Id"; Guid)
        {
            Caption = 'Batch Id';
            DataClassification = SystemMetadata;
            TableRelation = "PW Batch".Id;
        }
        field(2; "Chunk Index"; Integer)
        {
            Caption = 'Chunk Index';
            DataClassification = SystemMetadata;
        }
        field(3; Status; Enum "PW Chunk Status")
        {
            Caption = 'Status';
            DataClassification = SystemMetadata;
        }
        field(4; "Input Payload"; Blob)
        {
            Caption = 'Input Payload';
            DataClassification = SystemMetadata;
        }
        field(5; "Result Payload"; Blob)
        {
            Caption = 'Result Payload';
            DataClassification = SystemMetadata;
        }
        field(6; "Error Message"; Text[2048])
        {
            Caption = 'Error Message';
            DataClassification = SystemMetadata;
        }
        field(7; "Error Call Stack"; Blob)
        {
            Caption = 'Error Call Stack';
            DataClassification = SystemMetadata;
        }
        field(8; "Session Id"; Integer)
        {
            Caption = 'Session Id';
            DataClassification = SystemMetadata;
        }
        field(9; "Started At"; DateTime)
        {
            Caption = 'Started At';
            DataClassification = SystemMetadata;
        }
        field(10; "Completed At"; DateTime)
        {
            Caption = 'Completed At';
            DataClassification = SystemMetadata;
        }
    }

    keys
    {
        key(PK; "Batch Id", "Chunk Index")
        {
            Clustered = true;
        }
    }
}
