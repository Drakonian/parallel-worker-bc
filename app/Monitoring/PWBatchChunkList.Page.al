namespace VolodymyrDvernytskyi.ParallelWorker;

page 99001 "PW Batch Chunk List"
{
    ApplicationArea = All;
    Caption = 'PW Batch Chunks';
    PageType = List;
    SourceTable = "PW Batch Chunk";
    UsageCategory = None;
    Editable = false;

    layout
    {
        area(Content)
        {
            repeater(Chunks)
            {
                field("Batch Id"; Rec."Batch Id")
                {
                    ToolTip = 'Batch this chunk belongs to.';
                    Visible = false;
                }
                field("Chunk Index"; Rec."Chunk Index")
                {
                    ToolTip = 'Index of this chunk within the batch.';
                }
                field(Status; Rec.Status)
                {
                    ToolTip = 'Current status of this chunk.';
                }
                field("Session Id"; Rec."Session Id")
                {
                    ToolTip = 'Background session ID that executed this chunk.';
                }
                field("Started At"; Rec."Started At")
                {
                    ToolTip = 'When the chunk started executing.';
                }
                field("Completed At"; Rec."Completed At")
                {
                    ToolTip = 'When the chunk finished.';
                }
                field("Error Message"; Rec."Error Message")
                {
                    ToolTip = 'Error message if the chunk failed.';
                }
            }
        }
    }

    actions
    {
        area(Promoted)
        {
            actionref(ViewFullErrorRef; ViewFullError) { }
            actionref(ViewErrorCallStackRef; ViewErrorCallStack) { }
            actionref(ViewInputPayloadRef; ViewInputPayload) { }
            actionref(ViewResultPayloadRef; ViewResultPayload) { }
        }
        area(Processing)
        {
            action(ViewFullError)
            {
                Caption = 'View Full Error';
                ToolTip = 'Show the full error message for this chunk (not truncated).';
                Image = PrevErrorMessage;

                trigger OnAction()
                var
                    InStream: InStream;
                    TextBuilder: TextBuilder;
                    Line: Text;
                begin
                    Rec.CalcFields("Full Error Message");
                    if not Rec."Full Error Message".HasValue() then begin
                        if Rec."Error Message" <> '' then
                            Message(Rec."Error Message")
                        else
                            Message('No error message available.');
                        exit;
                    end;
                    Rec."Full Error Message".CreateInStream(InStream, TextEncoding::UTF8);
                    while not InStream.EOS() do begin
                        InStream.ReadText(Line);
                        TextBuilder.AppendLine(Line);
                    end;
                    Message(TextBuilder.ToText());
                end;
            }
            action(ViewErrorCallStack)
            {
                Caption = 'View Error Call Stack';
                ToolTip = 'Show the full error call stack for this chunk.';
                Image = Error;

                trigger OnAction()
                var
                    InStream: InStream;
                    TextBuilder: TextBuilder;
                    Line: Text;
                begin
                    Rec.CalcFields("Error Call Stack");
                    if not Rec."Error Call Stack".HasValue() then begin
                        Message('No error call stack available.');
                        exit;
                    end;
                    Rec."Error Call Stack".CreateInStream(InStream, TextEncoding::UTF8);
                    while not InStream.EOS() do begin
                        InStream.ReadText(Line);
                        TextBuilder.AppendLine(Line);
                    end;
                    Message(TextBuilder.ToText());
                end;
            }
            action(ViewInputPayload)
            {
                Caption = 'View Input Payload';
                ToolTip = 'Show the JSON input payload for this chunk.';
                Image = Document;

                trigger OnAction()
                var
                    InStream: InStream;
                    InputJson: JsonObject;
                    PayloadText: Text;
                begin
                    Rec.CalcFields("Input Payload");
                    if not Rec."Input Payload".HasValue() then begin
                        Message('No input payload available.');
                        exit;
                    end;
                    Rec."Input Payload".CreateInStream(InStream, TextEncoding::UTF8);
                    InputJson.ReadFrom(InStream);
                    InputJson.WriteTo(PayloadText);
                    Message(PayloadText);
                end;
            }
            action(ViewResultPayload)
            {
                Caption = 'View Result Payload';
                ToolTip = 'Show the JSON result payload for this chunk.';
                Image = Document;

                trigger OnAction()
                var
                    InStream: InStream;
                    ResultArray: JsonArray;
                    PayloadText: Text;
                begin
                    Rec.CalcFields("Result Payload");
                    if not Rec."Result Payload".HasValue() then begin
                        Message('No result payload available.');
                        exit;
                    end;
                    Rec."Result Payload".CreateInStream(InStream, TextEncoding::UTF8);
                    ResultArray.ReadFrom(InStream);
                    ResultArray.WriteTo(PayloadText);
                    Message(PayloadText);
                end;
            }
        }
    }
}
