codeunit 99201 "PW Chunk Context Test"
{
    Subtype = Test;
    TestPermissions = Disabled;

    var
        LibraryAssert: Codeunit "Library Assert";
        TestHelper: Codeunit "PW Test Helper";

    [Test]
    procedure InitSetsIdentifiers()
    // [SCENARIO 1.1] Init from chunk with JSON payload sets BatchId and ChunkIndex
    var
        Ctx: Codeunit "PW Chunk Context";
        Chunk: Record "PW Batch Chunk";
        BatchId: Guid;
        Payload: JsonObject;
    begin
        // [GIVEN] A batch with a chunk
        Payload.Add('Key1', 'Value1');
        BatchId := TestHelper.CreateBatch("PW Batch Status"::Running);
        TestHelper.CreateChunkWithPayload(BatchId, 3, Payload);

        // [WHEN] Init is called
        Chunk.Get(BatchId, 3);
        Ctx.Init(Chunk);

        // [THEN] BatchId and ChunkIndex are set correctly
        LibraryAssert.AreEqual(BatchId, Ctx.GetBatchId(), 'BatchId should match');
        LibraryAssert.AreEqual(3, Ctx.GetChunkIndex(), 'ChunkIndex should match');
    end;

    [Test]
    procedure GetTextInputReturnsCorrectValue()
    // [SCENARIO 1.2] GetTextInput reads a text value from payload
    var
        Ctx: Codeunit "PW Chunk Context";
        Chunk: Record "PW Batch Chunk";
        BatchId: Guid;
        Payload: JsonObject;
    begin
        Payload.Add('Name', 'Hello World');
        BatchId := TestHelper.CreateBatch("PW Batch Status"::Running);
        TestHelper.CreateChunkWithPayload(BatchId, 1, Payload);

        Chunk.Get(BatchId, 1);
        Ctx.Init(Chunk);

        LibraryAssert.AreEqual('Hello World', Ctx.GetTextInput('Name'), 'Text value should match');
    end;

    [Test]
    procedure GetIntInputReturnsCorrectValue()
    // [SCENARIO 1.3] GetIntInput reads an integer value from payload
    var
        Ctx: Codeunit "PW Chunk Context";
        Chunk: Record "PW Batch Chunk";
        BatchId: Guid;
        Payload: JsonObject;
    begin
        Payload.Add('Count', 42);
        BatchId := TestHelper.CreateBatch("PW Batch Status"::Running);
        TestHelper.CreateChunkWithPayload(BatchId, 1, Payload);

        Chunk.Get(BatchId, 1);
        Ctx.Init(Chunk);

        LibraryAssert.AreEqual(42, Ctx.GetIntInput('Count'), 'Integer value should match');
    end;

    [Test]
    procedure GetDecimalInputReturnsCorrectValue()
    // [SCENARIO 1.4] GetDecimalInput reads a decimal value from payload
    var
        Ctx: Codeunit "PW Chunk Context";
        Chunk: Record "PW Batch Chunk";
        BatchId: Guid;
        Payload: JsonObject;
    begin
        Payload.Add('Amount', 123.45);
        BatchId := TestHelper.CreateBatch("PW Batch Status"::Running);
        TestHelper.CreateChunkWithPayload(BatchId, 1, Payload);

        Chunk.Get(BatchId, 1);
        Ctx.Init(Chunk);

        LibraryAssert.AreNearlyEqual(123.45, Ctx.GetDecimalInput('Amount'), 0.001, 'Decimal value should match');
    end;

    [Test]
    procedure GetBoolInputReturnsCorrectValue()
    // [SCENARIO 1.5] GetBoolInput reads a boolean value from payload
    var
        Ctx: Codeunit "PW Chunk Context";
        Chunk: Record "PW Batch Chunk";
        BatchId: Guid;
        Payload: JsonObject;
    begin
        Payload.Add('IsActive', true);
        BatchId := TestHelper.CreateBatch("PW Batch Status"::Running);
        TestHelper.CreateChunkWithPayload(BatchId, 1, Payload);

        Chunk.Get(BatchId, 1);
        Ctx.Init(Chunk);

        LibraryAssert.IsTrue(Ctx.GetBoolInput('IsActive'), 'Boolean value should be true');
    end;

    [Test]
    procedure GetInputArrayReturnsCorrectArray()
    // [SCENARIO 1.6] GetInputArray reads a JSON array from payload
    var
        Ctx: Codeunit "PW Chunk Context";
        Chunk: Record "PW Batch Chunk";
        BatchId: Guid;
        Payload: JsonObject;
        Items: JsonArray;
        ResultArr: JsonArray;
    begin
        Items.Add('A');
        Items.Add('B');
        Items.Add('C');
        Payload.Add('$Items', Items);
        BatchId := TestHelper.CreateBatch("PW Batch Status"::Running);
        TestHelper.CreateChunkWithPayload(BatchId, 1, Payload);

        Chunk.Get(BatchId, 1);
        Ctx.Init(Chunk);
        ResultArr := Ctx.GetItems();

        LibraryAssert.AreEqual(3, ResultArr.Count(), 'Array should have 3 items');
    end;

    [Test]
    procedure GetInputReturnsFullPayload()
    // [SCENARIO 1.7] GetInput returns full JSON object with all keys
    var
        Ctx: Codeunit "PW Chunk Context";
        Chunk: Record "PW Batch Chunk";
        BatchId: Guid;
        Payload: JsonObject;
        Input: JsonObject;
    begin
        Payload.Add('Key1', 'Value1');
        Payload.Add('Key2', 42);
        BatchId := TestHelper.CreateBatch("PW Batch Status"::Running);
        TestHelper.CreateChunkWithPayload(BatchId, 1, Payload);

        Chunk.Get(BatchId, 1);
        Ctx.Init(Chunk);
        Input := Ctx.GetInput();

        LibraryAssert.IsTrue(Input.Contains('Key1'), 'Key1 should exist');
        LibraryAssert.IsTrue(Input.Contains('Key2'), 'Key2 should exist');
    end;

    [Test]
    procedure GetTextInputErrorsOnMissingKey()
    // [SCENARIO 1.8] GetTextInput with missing key raises error
    var
        Ctx: Codeunit "PW Chunk Context";
        Chunk: Record "PW Batch Chunk";
        BatchId: Guid;
        Payload: JsonObject;
    begin
        Payload.Add('OtherKey', 'Value');
        BatchId := TestHelper.CreateBatch("PW Batch Status"::Running);
        TestHelper.CreateChunkWithPayload(BatchId, 1, Payload);
        Chunk.Get(BatchId, 1);
        Ctx.Init(Chunk);

        asserterror Ctx.GetTextInput('MissingKey');

        LibraryAssert.ExpectedError('Key "MissingKey" not found in chunk input.');
    end;

    [Test]
    procedure GetIntInputErrorsOnMissingKey()
    // [SCENARIO 1.9] GetIntInput with missing key raises error
    var
        Ctx: Codeunit "PW Chunk Context";
        Chunk: Record "PW Batch Chunk";
        BatchId: Guid;
        Payload: JsonObject;
    begin
        Payload.Add('OtherKey', 'Value');
        BatchId := TestHelper.CreateBatch("PW Batch Status"::Running);
        TestHelper.CreateChunkWithPayload(BatchId, 1, Payload);
        Chunk.Get(BatchId, 1);
        Ctx.Init(Chunk);

        asserterror Ctx.GetIntInput('MissingKey');

        LibraryAssert.ExpectedError('Key "MissingKey" not found in chunk input.');
    end;

    [Test]
    procedure GetterErrorsBeforeInit()
    // [SCENARIO 1.10] Any getter called before Init raises error
    var
        Ctx: Codeunit "PW Chunk Context";
    begin
        asserterror Ctx.GetBatchId();

        LibraryAssert.ExpectedError('Chunk context has not been initialized.');
    end;

    [Test]
    procedure IsRecordChunkReturnsTrueForRecordPayload()
    // [SCENARIO 1.11] IsRecordChunk returns true when $IsRecordChunk is in payload
    var
        Ctx: Codeunit "PW Chunk Context";
        Chunk: Record "PW Batch Chunk";
        BatchId: Guid;
        Payload: JsonObject;
    begin
        Payload.Add('$IsRecordChunk', true);
        Payload.Add('$TableNo', 18);
        Payload.Add('$FilterView', '');
        Payload.Add('$StartIndex', 1);
        Payload.Add('$EndIndex', 10);
        BatchId := TestHelper.CreateBatch("PW Batch Status"::Running);
        TestHelper.CreateChunkWithPayload(BatchId, 1, Payload);

        Chunk.Get(BatchId, 1);
        Ctx.Init(Chunk);

        LibraryAssert.IsTrue(Ctx.IsRecordChunk(), 'Should be identified as record chunk');
    end;

    [Test]
    procedure IsRecordChunkReturnsFalseForListPayload()
    // [SCENARIO 1.12] IsRecordChunk returns false for list-based payload
    var
        Ctx: Codeunit "PW Chunk Context";
        Chunk: Record "PW Batch Chunk";
        BatchId: Guid;
        Payload: JsonObject;
        Items: JsonArray;
    begin
        Items.Add('Item1');
        Payload.Add('$Items', Items);
        BatchId := TestHelper.CreateBatch("PW Batch Status"::Running);
        TestHelper.CreateChunkWithPayload(BatchId, 1, Payload);

        Chunk.Get(BatchId, 1);
        Ctx.Init(Chunk);

        LibraryAssert.IsFalse(Ctx.IsRecordChunk(), 'Should not be identified as record chunk');
    end;

    [Test]
    procedure GetChunkSizeComputesCorrectly()
    // [SCENARIO 1.13] GetChunkSize computes EndIndex - StartIndex + 1
    var
        Ctx: Codeunit "PW Chunk Context";
        Chunk: Record "PW Batch Chunk";
        BatchId: Guid;
        Payload: JsonObject;
    begin
        Payload.Add('$IsRecordChunk', true);
        Payload.Add('$TableNo', 18);
        Payload.Add('$FilterView', '');
        Payload.Add('$StartIndex', 5);
        Payload.Add('$EndIndex', 15);
        BatchId := TestHelper.CreateBatch("PW Batch Status"::Running);
        TestHelper.CreateChunkWithPayload(BatchId, 1, Payload);

        Chunk.Get(BatchId, 1);
        Ctx.Init(Chunk);

        LibraryAssert.AreEqual(11, Ctx.GetChunkSize(), 'Chunk size should be EndIndex - StartIndex + 1');
    end;

    [Test]
    procedure GetRecordRefErrorsOnNonRecordChunk()
    // [SCENARIO 1.14] GetRecordRef on non-record chunk raises error
    var
        Ctx: Codeunit "PW Chunk Context";
        Chunk: Record "PW Batch Chunk";
        RecRef: RecordRef;
        BatchId: Guid;
        Payload: JsonObject;
    begin
        Payload.Add('Key1', 'Value1');
        BatchId := TestHelper.CreateBatch("PW Batch Status"::Running);
        TestHelper.CreateChunkWithPayload(BatchId, 1, Payload);
        Chunk.Get(BatchId, 1);
        Ctx.Init(Chunk);

        asserterror Ctx.GetRecordRef(RecRef);

        LibraryAssert.ExpectedError('This chunk was not created via RunForRecords.');
    end;

    [Test]
    procedure GetDecimalInputErrorsOnMissingKey()
    // [SCENARIO 1.20] GetDecimalInput with missing key raises error
    var
        Ctx: Codeunit "PW Chunk Context";
        Chunk: Record "PW Batch Chunk";
        BatchId: Guid;
        Payload: JsonObject;
    begin
        Payload.Add('OtherKey', 'Value');
        BatchId := TestHelper.CreateBatch("PW Batch Status"::Running);
        TestHelper.CreateChunkWithPayload(BatchId, 1, Payload);
        Chunk.Get(BatchId, 1);
        Ctx.Init(Chunk);

        asserterror Ctx.GetDecimalInput('MissingKey');

        LibraryAssert.ExpectedError('Key "MissingKey" not found in chunk input.');
    end;

    [Test]
    procedure GetBoolInputErrorsOnMissingKey()
    // [SCENARIO 1.21] GetBoolInput with missing key raises error
    var
        Ctx: Codeunit "PW Chunk Context";
        Chunk: Record "PW Batch Chunk";
        BatchId: Guid;
        Payload: JsonObject;
    begin
        Payload.Add('OtherKey', 'Value');
        BatchId := TestHelper.CreateBatch("PW Batch Status"::Running);
        TestHelper.CreateChunkWithPayload(BatchId, 1, Payload);
        Chunk.Get(BatchId, 1);
        Ctx.Init(Chunk);

        asserterror Ctx.GetBoolInput('MissingKey');

        LibraryAssert.ExpectedError('Key "MissingKey" not found in chunk input.');
    end;

    [Test]
    procedure GetInputArrayErrorsOnMissingKey()
    // [SCENARIO 1.22] GetInputArray with missing key raises error
    var
        Ctx: Codeunit "PW Chunk Context";
        Chunk: Record "PW Batch Chunk";
        BatchId: Guid;
        Payload: JsonObject;
    begin
        Payload.Add('OtherKey', 'Value');
        BatchId := TestHelper.CreateBatch("PW Batch Status"::Running);
        TestHelper.CreateChunkWithPayload(BatchId, 1, Payload);
        Chunk.Get(BatchId, 1);
        Ctx.Init(Chunk);

        asserterror Ctx.GetInputArray('MissingKey');

        LibraryAssert.ExpectedError('Key "MissingKey" not found in chunk input.');
    end;

    [Test]
    procedure SetResultStoresSingleResult()
    // [SCENARIO 1.15] SetResult stores single result readable after SaveResultsTo
    var
        Ctx: Codeunit "PW Chunk Context";
        Chunk: Record "PW Batch Chunk";
        BatchId: Guid;
        Payload: JsonObject;
        Result: JsonObject;
        ResultArray: JsonArray;
        InStream: InStream;
    begin
        Payload.Add('Key1', 'Value1');
        BatchId := TestHelper.CreateBatch("PW Batch Status"::Running);
        TestHelper.CreateChunkWithPayload(BatchId, 1, Payload);
        Chunk.Get(BatchId, 1);
        Ctx.Init(Chunk);

        Result.Add('Count', 5);
        Ctx.SetResult(Result);
        Ctx.SaveResultsTo(Chunk);
        Chunk.Modify();

        Chunk.CalcFields("Result Payload");
        Chunk."Result Payload".CreateInStream(InStream, TextEncoding::UTF8);
        ResultArray.ReadFrom(InStream);
        LibraryAssert.AreEqual(1, ResultArray.Count(), 'Should have 1 result');
    end;

    [Test]
    procedure SetResultReplacesPreviousResult()
    // [SCENARIO 1.16] Calling SetResult twice keeps only the last result
    var
        Ctx: Codeunit "PW Chunk Context";
        Chunk: Record "PW Batch Chunk";
        BatchId: Guid;
        Payload: JsonObject;
        Result1: JsonObject;
        Result2: JsonObject;
        ResultArray: JsonArray;
        ResultObj: JsonObject;
        Token: JsonToken;
        VersionToken: JsonToken;
        InStream: InStream;
    begin
        Payload.Add('Key1', 'Value1');
        BatchId := TestHelper.CreateBatch("PW Batch Status"::Running);
        TestHelper.CreateChunkWithPayload(BatchId, 1, Payload);
        Chunk.Get(BatchId, 1);
        Ctx.Init(Chunk);

        Result1.Add('Version', 1);
        Ctx.SetResult(Result1);
        Result2.Add('Version', 2);
        Ctx.SetResult(Result2);
        Ctx.SaveResultsTo(Chunk);
        Chunk.Modify();

        Chunk.CalcFields("Result Payload");
        Chunk."Result Payload".CreateInStream(InStream, TextEncoding::UTF8);
        ResultArray.ReadFrom(InStream);
        LibraryAssert.AreEqual(1, ResultArray.Count(), 'Should have 1 result');
        ResultArray.Get(0, Token);
        ResultObj := Token.AsObject();
        ResultObj.Get('Version', VersionToken);
        LibraryAssert.AreEqual(2, VersionToken.AsValue().AsInteger(), 'Should be the second result');
    end;

    [Test]
    procedure AppendResultAccumulatesResults()
    // [SCENARIO 1.17] AppendResult called 3 times — array contains 3 objects
    var
        Ctx: Codeunit "PW Chunk Context";
        Chunk: Record "PW Batch Chunk";
        BatchId: Guid;
        Payload: JsonObject;
        Result: JsonObject;
        ResultArray: JsonArray;
        InStream: InStream;
        i: Integer;
    begin
        Payload.Add('Key1', 'Value1');
        BatchId := TestHelper.CreateBatch("PW Batch Status"::Running);
        TestHelper.CreateChunkWithPayload(BatchId, 1, Payload);
        Chunk.Get(BatchId, 1);
        Ctx.Init(Chunk);

        for i := 1 to 3 do begin
            Clear(Result);
            Result.Add('Index', i);
            Ctx.AppendResult(Result);
        end;
        Ctx.SaveResultsTo(Chunk);
        Chunk.Modify();

        Chunk.CalcFields("Result Payload");
        Chunk."Result Payload".CreateInStream(InStream, TextEncoding::UTF8);
        ResultArray.ReadFrom(InStream);
        LibraryAssert.AreEqual(3, ResultArray.Count(), 'Should have 3 results');
    end;

    [Test]
    procedure SetResultThenAppendResultGivesTwoResults()
    // [SCENARIO 1.18] SetResult then AppendResult — array contains 2 objects
    var
        Ctx: Codeunit "PW Chunk Context";
        Chunk: Record "PW Batch Chunk";
        BatchId: Guid;
        Payload: JsonObject;
        Result1: JsonObject;
        Result2: JsonObject;
        ResultArray: JsonArray;
        InStream: InStream;
    begin
        Payload.Add('Key1', 'Value1');
        BatchId := TestHelper.CreateBatch("PW Batch Status"::Running);
        TestHelper.CreateChunkWithPayload(BatchId, 1, Payload);
        Chunk.Get(BatchId, 1);
        Ctx.Init(Chunk);

        Result1.Add('First', true);
        Ctx.SetResult(Result1);
        Result2.Add('Second', true);
        Ctx.AppendResult(Result2);
        Ctx.SaveResultsTo(Chunk);
        Chunk.Modify();

        Chunk.CalcFields("Result Payload");
        Chunk."Result Payload".CreateInStream(InStream, TextEncoding::UTF8);
        ResultArray.ReadFrom(InStream);
        LibraryAssert.AreEqual(2, ResultArray.Count(), 'Should have 2 results');
    end;

    [Test]
    procedure SaveResultsToWithNoResultsLeavesEmptyBlob()
    // [SCENARIO 1.19] SaveResultsTo with no results — blob stays empty
    var
        Ctx: Codeunit "PW Chunk Context";
        Chunk: Record "PW Batch Chunk";
        BatchId: Guid;
        Payload: JsonObject;
    begin
        Payload.Add('Key1', 'Value1');
        BatchId := TestHelper.CreateBatch("PW Batch Status"::Running);
        TestHelper.CreateChunkWithPayload(BatchId, 1, Payload);
        Chunk.Get(BatchId, 1);
        Ctx.Init(Chunk);

        Ctx.SaveResultsTo(Chunk);
        Chunk.Modify();

        Chunk.CalcFields("Result Payload");
        LibraryAssert.IsFalse(Chunk."Result Payload".HasValue(), 'Result blob should be empty');
    end;
}
