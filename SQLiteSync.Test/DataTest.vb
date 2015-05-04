Imports System.Data
Imports System.IO
#If PocketPC Then
Imports AsGoodAsItGets.System.Runtime.Serialization.Formatters.Binary
#Else
Imports System.Runtime.Serialization.Formatters.Binary
#End If
Imports Microsoft.Synchronization.Data
Imports System.Data.SQLite
Imports System.Reflection
Imports Microsoft.VisualStudio.TestTools.UnitTesting


<TestClass()> _
Public Class DataTest
    Private _Connection As SQLiteConnection
    Private _SqliteClientSyncProvider As SqliteClientSyncProvider

    Private _SyncSession As SyncSession

    Private AssemblyName As String = Assembly.GetExecutingAssembly.GetName.Name

    Private _
        _SyncTable As SyncTable = _
            Serializator(Of SyncTable).LoadFromStream( _
                                                        Assembly.GetExecutingAssembly.GetManifestResourceStream( _
                                                                                                                 AssemblyName & _
                                                                                                                 ".test_SyncTable.xml"))

    '.Load(GetPath() & "test_synctable.xml")
    Private _
        _SyncSchema As SyncSchema = _
            Serializator(Of SyncSchema).LoadFromStream( _
                                                         Assembly.GetExecutingAssembly.GetManifestResourceStream( _
                                                                                                                  AssemblyName & _
                                                                                                                  ".test_SyncSchema.xml"))

    Private _EventRaisedOnUpdateConflict As Boolean = False
    Private _EventRaisedOnDeleteConflict As Boolean = False
    Private _EventRaisedOnInsertConflict As Boolean = False

    Private _ResoluciontAction As ApplyAction

    'Private _InitialSentAnchor As SyncAnchor
    'Private _NewSentAnchor As SyncAnchor

    'Private _InitialReceivedAnchor As SyncAnchor
    'Private _NewReceivedAnchor As SyncAnchor

    Private _GroupMetadata As New SyncGroupMetadata
    Private _TableMetadata As New SyncTableMetadata


    <TestInitialize()> _
    Public Sub Setup()
        _Connection = New SQLiteConnection("Data source=:memory:")
        _Connection.Open()

        _SqliteClientSyncProvider = New SqliteClientSyncProvider(Me._Connection)

        _SyncSession = New SyncSession

        CreateGroupMetadata()
    End Sub

    Public Sub CreateSchema(ByVal SyncDir As SyncDirection)
        'Set the direction
        Me._SyncTable.SyncDirection = SyncDir
        Me._TableMetadata.SyncDirection = SyncDir

        Me._SqliteClientSyncProvider.BeginTransaction(_SyncSession)
        Me._SqliteClientSyncProvider.CreateSchema(_SyncTable, _SyncSchema)
        Me._SqliteClientSyncProvider.EndTransaction(True, _SyncSession)
    End Sub

    Public Sub CreateGroupMetadata()
        'Create GroupMetadata and TableMetadata
        Me._GroupMetadata = New SyncGroupMetadata
        Me._TableMetadata = New SyncTableMetadata(Me._SyncTable.TableName, Me._SyncTable.SyncDirection)
        Me._GroupMetadata.TablesMetadata.Add(Me._TableMetadata)
    End Sub

    <TestCleanup()> _
    Public Sub TearDown()

        If Me._Connection IsNot Nothing AndAlso Me._Connection.State = ConnectionState.Open Then
            Me._Connection.Close()
        End If
        '_Connection.Dispose() ' done by the provider
        _SqliteClientSyncProvider.Dispose()
        _SqliteClientSyncProvider = Nothing
    End Sub


    Public Function DoGetChanges(ByVal SyncDir As SyncDirection) As DataSet

        Dim SentAnchor As SyncAnchor
        Dim ReceivedAnchor As SyncAnchor

        Me._SqliteClientSyncProvider.BeginTransaction(Me._SyncSession)

        'Retrieve SentAnchor
        SentAnchor = Me._SqliteClientSyncProvider.GetTableSentAnchor(Me._SyncTable.TableName)
        ReceivedAnchor = Me._SqliteClientSyncProvider.GetTableReceivedAnchor(Me._SyncTable.TableName)


        Me._SqliteClientSyncProvider.EndTransaction(True, Me._SyncSession)


        'Create the schema if not exit.
        If SentAnchor Is Nothing Then
            CreateSchema(SyncDir)
            SentAnchor = Me._SqliteClientSyncProvider.GetTableSentAnchor(Me._SyncTable.TableName)
            ReceivedAnchor = Me._SqliteClientSyncProvider.GetTableReceivedAnchor(Me._SyncTable.TableName)
        End If

        'Set the anchors.
        With Me._TableMetadata
            .LastSentAnchor = SentAnchor
            .LastReceivedAnchor = ReceivedAnchor
        End With

        'Get changes
        Dim SyncContext As SyncContext = Me._SqliteClientSyncProvider.GetChanges(Me._GroupMetadata, Me._SyncSession)


        'Begin transaction
        Me._SqliteClientSyncProvider.BeginTransaction(Me._SyncSession)
        'Save the new anchor.
        Me._SqliteClientSyncProvider.SetTableSentAnchor(Me._SyncTable.TableName, SyncContext.NewAnchor)
        'End transaction
        Me._SqliteClientSyncProvider.EndTransaction(True, Me._SyncSession)


        Return SyncContext.DataSet
    End Function

    Public Function DoApplyChanges(ByVal Data As DataSet) As SyncContext
        Dim ReceivedAnchor As SyncAnchor
        Dim Result As SyncContext
        Dim Anchor As Long

        Me._SqliteClientSyncProvider.BeginTransaction(Me._SyncSession)


        'Retrieve last received anchor.
        ReceivedAnchor = Me._SqliteClientSyncProvider.GetTableReceivedAnchor("TestTable")
        Me._GroupMetadata.NewAnchor = ReceivedAnchor

        'Cast the anchor to long
        Anchor = CType(DeserializeAnchorValue(ReceivedAnchor.Anchor), Long)

        'Create the new anchor
        Anchor += 1
        Me._GroupMetadata.NewAnchor = New SyncAnchor(SerializeAnchorValue(Anchor))

        'ApplyChanges
        Result = Me._SqliteClientSyncProvider.ApplyChanges(Me._GroupMetadata, Data, Me._SyncSession)

        'end transaction
        Me._SqliteClientSyncProvider.EndTransaction(True, Me._SyncSession)

        Return Result
    End Function

    Public Function GetEmptyDataset() As DataSet
        Dim Data As New DataSet
        With Data.Tables.Add("TestTable")
            .Columns.Add("TestTable_Id", GetType(Integer))
            .Columns.Add("TestTable_Date", GetType(DateTime))
        End With
        Return Data
    End Function

    Public Function GetDatasetToSend(ByVal Id As Integer, ByVal TestDate As Date) As DataSet
        Dim Data As DataSet = GetEmptyDataset()
        Data.Tables(0).Rows.Add(Id, TestDate)
        Return Data
    End Function

    Public Function GetActualRecordCount(Optional ByVal Id As Integer = -1) As Integer
        Try
            Using _
                CMD As _
                    New SQLiteCommand("SELECT count(*) FROM TestTable WHERE TestTable_Id = @Id or @Id = -1", _
                                       Me._Connection)
                CMD.Parameters.Add("@Id", DbType.Int32).Value = Id
                Return CMD.ExecuteScalar
            End Using
        Catch ex As Exception
            Return 0
        End Try

    End Function

    ''' <summary>
    ''' Insert one record and sync.
    ''' </summary>
    ''' <param name="SyncDir"></param>
    ''' <returns>Id for the inserted record</returns>
    ''' <remarks></remarks>
    Private Function InsertRecordAndSync(ByVal SyncDir As SyncDirection) As Integer
        'Dim DataRetrieved As DataSet
        Dim IdClientInsert As Integer = 4
        ' Dim IdServerInsert As Integer = IdClientInsert 'RndGenerator.Next(11, 20)
        CreateSchema(SyncDir)

        'Simulate client insert
        Using _
            CMD As _
                New SQLiteCommand("INSERT INTO TestTable(TestTable_Id,TestTable_Date) values(@Id, @DateValue )", _
                                   Me._Connection)
            CMD.Parameters.Add("@Id", DbType.Int32).Value = IdClientInsert
            CMD.Parameters.Add("@DateValue", DbType.DateTime).Value = DateTime.Now
            CMD.ExecuteNonQuery()
        End Using

        'Server get changes.
        DoGetChanges(SyncDir)
        Return IdClientInsert
    End Function


    ''' <summary>
    ''' Test get changes and apply changes with no changes in the client nor in the server.
    ''' All other fixtures end with this test to ensure the sync state.
    ''' </summary>
    ''' <param name="SyncDir"></param>
    ''' <remarks></remarks>
    Public Sub NoChangesTest(ByVal SyncDir As SyncDirection)
        Dim InitialRecordCount As Integer = GetActualRecordCount()
        Dim ActualRecordCount As Integer

        Dim DataRetrieved As DataSet

        DataRetrieved = DoGetChanges(SyncDir)

        'Asserts for getchanges
        Select Case SyncDir
            Case SyncDirection.Bidirectional, SyncDirection.UploadOnly
                Assert.AreEqual(1, DataRetrieved.Tables.Count)
                Assert.AreEqual(0, DataRetrieved.Tables(Me._SyncTable.TableName).Rows.Count)
            Case SyncDirection.DownloadOnly, SyncDirection.Snapshot
                Assert.AreEqual(0, DataRetrieved.Tables.Count)
        End Select


        'Apply changes with empty dataset.
        Dim SyncContext As SyncContext = DoApplyChanges(GetEmptyDataset())


        'Check if the table is still empty.
        ActualRecordCount = GetActualRecordCount()

        'Asserts for apply change
        Assert.AreEqual(0, SyncContext.GroupProgress.TotalChanges)
        Assert.AreEqual(0, SyncContext.GroupProgress.TotalChangesApplied)
        Assert.AreEqual(0, SyncContext.GroupProgress.TotalChangesPending)
        Assert.AreEqual(0, SyncContext.GroupProgress.TotalDeletes)
        Assert.AreEqual(0, SyncContext.GroupProgress.TotalInserts)
        Assert.AreEqual(0, SyncContext.GroupProgress.TotalUpdates)

        If SyncDir <> SyncDirection.Snapshot Then
            Assert.AreEqual(InitialRecordCount, ActualRecordCount)
        Else
            'Apply changes with empty dataset will clean the table.
            Assert.AreEqual(0, ActualRecordCount)
        End If
    End Sub

    <TestMethod()> _
    Public Sub NoChangesTestSnapshot()
        NoChangesTest(SyncDirection.Snapshot)
    End Sub
    <TestMethod()> _
    Public Sub NoChangesTestBidirectional()
        NoChangesTest(SyncDirection.Bidirectional)
    End Sub
    <TestMethod()> _
    Public Sub NoChangesTestDownloadOnly()
        NoChangesTest(SyncDirection.DownloadOnly)
    End Sub
    <TestMethod()> _
    Public Sub NoChangesTestUploadOnly()
        NoChangesTest(SyncDirection.UploadOnly)
    End Sub

    ''' <summary>
    ''' Client insert a row and the server has no changes.
    ''' </summary>
    ''' <param name="SyncDir"></param>
    ''' <remarks></remarks>
    Public Sub ClientInsertTest(ByVal SyncDir As SyncDirection)

        CreateSchema(SyncDir)


        'Simulate client insert
        Dim RndGenerator As New Random
        Dim IdTestValue As Integer = RndGenerator.Next(1, 10)
        Using _
            CMD As _
                New SQLiteCommand("INSERT INTO TestTable(TestTable_Id,TestTable_Date) values(@Id, @DateValue )", _
                                   Me._Connection)
            CMD.Parameters.Add("@Id", DbType.Int32).Value = IdTestValue
            CMD.Parameters.Add("@DateValue", DbType.DateTime).Value = DateTime.Now
            CMD.ExecuteNonQuery()
        End Using

        Dim DataRetrieved As DataSet = DoGetChanges(SyncDir)

        'Asserts for GetChange
        Select Case SyncDir
            Case SyncDirection.Bidirectional, SyncDirection.UploadOnly
                Assert.AreEqual(1, DataRetrieved.Tables.Count)
                Assert.AreEqual(1, DataRetrieved.Tables(Me._SyncTable.TableName).Rows.Count)
                Assert.AreEqual(IdTestValue, _
                                 CType(DataRetrieved.Tables(Me._SyncTable.TableName).Rows(0).Item(0), Integer))
                Assert.AreEqual(DataRowState.Added, DataRetrieved.Tables(Me._SyncTable.TableName).Rows(0).RowState)
            Case SyncDirection.DownloadOnly, SyncDirection.Snapshot
                Assert.AreEqual(0, DataRetrieved.Tables.Count)
        End Select


        'Create Dataset
        DoApplyChanges(GetEmptyDataset)

        'Check if the table has the record.

        Dim ExistInsertedRecord As Boolean = (GetActualRecordCount(IdTestValue) = 1)

        If SyncDir = SyncDirection.Snapshot Then
            Assert.IsFalse(ExistInsertedRecord)
        Else
            Assert.IsTrue(ExistInsertedRecord)
        End If

        'Check if GetChanges return any row (supposed no)
        NoChangesTest(SyncDir)
        ''

    End Sub

    <TestMethod()> _
    Public Sub ClientInsertTestSnapshot()
        ClientInsertTest(SyncDirection.Snapshot)
    End Sub
    <TestMethod()> _
    Public Sub ClientInsertTestBidirectional()
        ClientInsertTest(SyncDirection.Bidirectional)
    End Sub
    <TestMethod()> _
    Public Sub ClientInsertTestDownloadOnly()
        ClientInsertTest(SyncDirection.DownloadOnly)
    End Sub
    <TestMethod()> _
    Public Sub ClientInsertTestUploadOnly()
        ClientInsertTest(SyncDirection.UploadOnly)
    End Sub

    ''' <summary>
    ''' Server has an insert to sync with the client.
    ''' </summary>
    ''' <param name="SyncDir"></param>
    ''' <remarks></remarks>
    Public Sub ServerInsertTest(ByVal SyncDir As SyncDirection)
        Dim DataRetrieved As DataSet
        Dim RndGenerator As New Random
        Dim RecordCount As Integer
        Dim IdTestValue As Integer = RndGenerator.Next(1, 10)

        'Get changes
        DataRetrieved = DoGetChanges(SyncDir)


        'Asserts for get changes.
        Select Case SyncDir
            Case SyncDirection.Bidirectional, SyncDirection.UploadOnly
                Assert.AreEqual(1, DataRetrieved.Tables.Count)
                Assert.AreEqual(0, DataRetrieved.Tables(Me._SyncTable.TableName).Rows.Count)
            Case SyncDirection.DownloadOnly, SyncDirection.Snapshot
                Assert.AreEqual(0, DataRetrieved.Tables.Count)
        End Select


        'Apply Changes.
        Dim SyncContext As SyncContext = DoApplyChanges(GetDatasetToSend(IdTestValue, DateTime.Now))

        'Asserts for apply changes
        If Not SyncDir = SyncDirection.UploadOnly Then
            Assert.AreEqual(1, SyncContext.GroupProgress.TotalChanges)
            Assert.AreEqual(1, SyncContext.GroupProgress.TotalChangesApplied)
            Assert.AreEqual(0, SyncContext.GroupProgress.TotalChangesPending)
            Assert.AreEqual(0, SyncContext.GroupProgress.TotalDeletes)
            Assert.AreEqual(1, SyncContext.GroupProgress.TotalInserts)
            Assert.AreEqual(0, SyncContext.GroupProgress.TotalUpdates)
        End If

        'Check if the table has the record.
        RecordCount = GetActualRecordCount(IdTestValue)

        If SyncDir = SyncDirection.UploadOnly Then
            Assert.AreEqual(0, RecordCount)
        Else
            Assert.AreEqual(1, RecordCount)
        End If

        'Check if GetChanges return any row (supposed no)
        NoChangesTest(SyncDir)
    End Sub

    <TestMethod()> _
    Public Sub ServerInsertTestSnapshot()
        ServerInsertTest(SyncDirection.Snapshot)
    End Sub
    <TestMethod()> _
    Public Sub ServerInsertTestBidirectional()
        ServerInsertTest(SyncDirection.Bidirectional)
    End Sub
    <TestMethod()> _
    Public Sub ServerInsertTestSnapshotDownloadOnly()
        ServerInsertTest(SyncDirection.DownloadOnly)
    End Sub
    <TestMethod()> _
    Public Sub ServerInsertTestSnapshotUploadOnly()
        ServerInsertTest(SyncDirection.UploadOnly)
    End Sub

    ''' <summary>
    ''' Client has made insert and server has an insert to apply in the client.
    ''' </summary>
    ''' <param name="SyncDir"></param>
    ''' <remarks></remarks>
    Public Sub ServerInsertClientInsertTest(ByVal SyncDir As SyncDirection)
        Dim DataRetrieved As DataSet
        Dim RndGenerator As New Random
        Dim RecordCount As Integer
        Dim IdClientInsert As Integer = RndGenerator.Next(1, 10)
        Dim IdServerInsert As Integer = RndGenerator.Next(11, 20)

        CreateSchema(SyncDir)

        'Simulate client insert

        Using _
            CMD As _
                New SQLiteCommand("INSERT INTO TestTable(TestTable_Id,TestTable_Date) values(@Id, @DateValue )", _
                                   Me._Connection)
            CMD.Parameters.Add("@Id", DbType.Int32).Value = IdClientInsert
            CMD.Parameters.Add("@DateValue", DbType.DateTime).Value = DateTime.Now
            CMD.ExecuteNonQuery()
        End Using

        'Get changes
        DataRetrieved = DoGetChanges(SyncDir)

        'Asserts for get changes.
        Select Case SyncDir
            Case SyncDirection.Bidirectional, SyncDirection.UploadOnly
                Assert.AreEqual(1, DataRetrieved.Tables.Count)
                Assert.AreEqual(1, DataRetrieved.Tables(Me._SyncTable.TableName).Rows.Count)
                Assert.AreEqual(IdClientInsert, _
                                 CType(DataRetrieved.Tables(Me._SyncTable.TableName).Rows(0).Item(0), Integer))
                Assert.AreEqual(DataRowState.Added, DataRetrieved.Tables(Me._SyncTable.TableName).Rows(0).RowState)
            Case SyncDirection.DownloadOnly, SyncDirection.Snapshot
                Assert.AreEqual(0, DataRetrieved.Tables.Count)
        End Select

        'Apply Changes.
        Dim SyncContext As SyncContext = DoApplyChanges(GetDatasetToSend(IdServerInsert, DateTime.Now))


        'Check if the table has the record.
        If Not SyncDir = SyncDirection.UploadOnly Then
            Assert.AreEqual(1, SyncContext.GroupProgress.TotalChanges)
            Assert.AreEqual(1, SyncContext.GroupProgress.TotalChangesApplied)
            Assert.AreEqual(0, SyncContext.GroupProgress.TotalChangesPending)
            Assert.AreEqual(0, SyncContext.GroupProgress.TotalDeletes)
            Assert.AreEqual(1, SyncContext.GroupProgress.TotalInserts)
            Assert.AreEqual(0, SyncContext.GroupProgress.TotalUpdates)
        End If
        RecordCount = GetActualRecordCount(IdServerInsert)

        If SyncDir = SyncDirection.UploadOnly Then
            Assert.AreEqual(0, RecordCount)
        Else
            Assert.AreEqual(1, RecordCount)
        End If

        If SyncDir = SyncDirection.Snapshot Or SyncDir = SyncDirection.UploadOnly Then
            Assert.AreEqual(1, GetActualRecordCount())
        Else
            Assert.AreEqual(2, GetActualRecordCount())
        End If

        'Check if GetChanges return any row (supposed no)
        NoChangesTest(SyncDir)
    End Sub

    <TestMethod()> _
    Public Sub ServerInsertClientInsertTestBidirectional()
        ServerInsertClientInsertTest(SyncDirection.Bidirectional)
    End Sub
    <TestMethod()> _
    Public Sub ServerInsertClientInsertTestDownloadOnly()
        ServerInsertClientInsertTest(SyncDirection.DownloadOnly)
    End Sub
    <TestMethod()> _
    Public Sub ServerInsertClientInsertTestSnapshot()
        ServerInsertClientInsertTest(SyncDirection.Snapshot)
    End Sub
    <TestMethod()> _
    Public Sub ServerInsertClientInsertTestUploadOnly()
        ServerInsertClientInsertTest(SyncDirection.UploadOnly)
    End Sub

    ''' <summary>
    ''' Client and server insert a row with same PK.
    ''' </summary>
    ''' <param name="SyncDir"></param>
    ''' <param name="ApplyAction"></param>
    ''' <remarks></remarks>
    Public Sub ServerInsertClientInsertConflictTest(ByVal SyncDir As SyncDirection, ByVal ApplyAction As ApplyAction)
        'Dim DataRetrieved As DataSet
        Dim IdClientInsert As Integer = 4
        Dim IdServerInsert As Integer = IdClientInsert
        'RndGenerator.Next(11, 20)

        CreateSchema(SyncDir)

        'Simulate client insert

        Using _
            CMD As _
                New SQLiteCommand("INSERT INTO TestTable(TestTable_Id,TestTable_Date) values(@Id, @DateValue )", _
                                   Me._Connection)
            CMD.Parameters.Add("@Id", DbType.Int32).Value = IdClientInsert
            CMD.Parameters.Add("@DateValue", DbType.DateTime).Value = DateTime.Now
            CMD.ExecuteNonQuery()
        End Using

        Me._ResoluciontAction = ApplyAction

        Me._EventRaisedOnInsertConflict = False

        'Apply Changes.
        AddHandler _SqliteClientSyncProvider.ApplyChangeFailed, AddressOf OnApplyChangeInsertConflict

        Dim SyncContext As SyncContext = DoApplyChanges(GetDatasetToSend(IdServerInsert, DateTime.Now))

        Assert.AreEqual((SyncDir = SyncDirection.Bidirectional Or SyncDir = SyncDirection.DownloadOnly), _
                         Me._EventRaisedOnInsertConflict)

        If SyncDir = SyncDirection.UploadOnly Or SyncDir = SyncDirection.Snapshot Then Exit Sub

        If ApplyAction = Microsoft.Synchronization.Data.ApplyAction.Continue Then
            Assert.AreEqual(1, SyncContext.GroupProgress.TotalChanges)
            Assert.AreEqual(1, SyncContext.GroupProgress.TotalChangesFailed)
            Assert.AreEqual(0, SyncContext.GroupProgress.TotalChangesApplied)
            Assert.AreEqual(0, SyncContext.GroupProgress.TotalChangesPending)
            Assert.AreEqual(0, SyncContext.GroupProgress.TotalDeletes)
            Assert.AreEqual(1, SyncContext.GroupProgress.TotalInserts)
            Assert.AreEqual(0, SyncContext.GroupProgress.TotalUpdates)
        ElseIf _
            ApplyAction = Microsoft.Synchronization.Data.ApplyAction.RetryWithForceWrite Or _
            ApplyAction = Microsoft.Synchronization.Data.ApplyAction.RetryApplyingRow Then
            Assert.AreEqual(1, SyncContext.GroupProgress.TotalChanges)
            Assert.AreEqual(0, SyncContext.GroupProgress.TotalChangesFailed)
            Assert.AreEqual(1, SyncContext.GroupProgress.TotalChangesApplied)
            Assert.AreEqual(0, SyncContext.GroupProgress.TotalChangesPending)
            Assert.AreEqual(0, SyncContext.GroupProgress.TotalDeletes)
            Assert.AreEqual(1, SyncContext.GroupProgress.TotalInserts)
            Assert.AreEqual(0, SyncContext.GroupProgress.TotalUpdates)
        End If
        If ApplyAction = Microsoft.Synchronization.Data.ApplyAction.RetryApplyingRow Then
            Assert.AreEqual(2, GetActualRecordCount())
        End If
        If SyncDir = SyncDirection.Bidirectional Then
            DoGetChanges(SyncDir)
        End If
        NoChangesTest(SyncDir)
    End Sub

    <TestMethod()> _
    Public Sub ServerInsertClientInsertConflictTest_Snapshot_Continue()
        ServerInsertClientInsertConflictTest(SyncDirection.Snapshot, ApplyAction.Continue)
    End Sub
    <TestMethod()> _
    Public Sub ServerInsertClientInsertConflictTest_Snapshot_RetryApplyingRow()
        ServerInsertClientInsertConflictTest(SyncDirection.Snapshot, ApplyAction.RetryApplyingRow)
    End Sub
    <TestMethod()> _
    Public Sub ServerInsertClientInsertConflictTest_Snapshot_RetryWithForceWrite()
        ServerInsertClientInsertConflictTest(SyncDirection.Snapshot, ApplyAction.RetryWithForceWrite)
    End Sub
    <TestMethod()> _
    Public Sub ServerInsertClientInsertConflictTest_Bidirectional_Continue()
        ServerInsertClientInsertConflictTest(SyncDirection.Bidirectional, ApplyAction.Continue)
    End Sub
    <TestMethod()> _
    Public Sub ServerInsertClientInsertConflictTest_Bidirectional_RetryApplyingRow()
        ServerInsertClientInsertConflictTest(SyncDirection.Bidirectional, ApplyAction.RetryApplyingRow)
    End Sub
    <TestMethod()> _
    Public Sub ServerInsertClientInsertConflictTest_Bidirectional_RetryWithForceWrite()
        ServerInsertClientInsertConflictTest(SyncDirection.Bidirectional, ApplyAction.RetryWithForceWrite)
    End Sub

    <TestMethod()> _
    Public Sub ServerInsertClientInsertConflictTest_DownloadOnly_Continue()
        ServerInsertClientInsertConflictTest(SyncDirection.DownloadOnly, ApplyAction.Continue)
    End Sub
    <TestMethod()> _
    Public Sub ServerInsertClientInsertConflictTest_DownloadOnly_RetryApplyingRow()
        ServerInsertClientInsertConflictTest(SyncDirection.DownloadOnly, ApplyAction.RetryApplyingRow)
    End Sub
    <TestMethod()> _
    Public Sub ServerInsertClientInsertConflictTest_DownloadOnly_RetryWithForceWrite()
        ServerInsertClientInsertConflictTest(SyncDirection.DownloadOnly, ApplyAction.RetryWithForceWrite)
    End Sub
    <TestMethod()> _
    Public Sub ServerInsertClientInsertConflictTest_UploadOnly_Continue()
        ServerInsertClientInsertConflictTest(SyncDirection.UploadOnly, ApplyAction.Continue)
    End Sub
    <TestMethod()> _
    Public Sub ServerInsertClientInsertConflictTest_UploadOnly_RetryApplyingRow()
        ServerInsertClientInsertConflictTest(SyncDirection.UploadOnly, ApplyAction.RetryApplyingRow)
    End Sub
    <TestMethod()> _
    Public Sub ServerInsertClientInsertConflictTest_UploadOnly_RetryWithForceWrite()
        ServerInsertClientInsertConflictTest(SyncDirection.UploadOnly, ApplyAction.RetryWithForceWrite)
    End Sub


    Public Sub OnApplyChangeInsertConflict(ByVal sender As Object, ByVal e As ApplyChangeFailedEventArgs)

        Assert.AreEqual(ConflictType.ClientInsertServerInsert, e.Conflict.ConflictType)
        e.Action = Me._ResoluciontAction
        If e.Action = ApplyAction.RetryApplyingRow Then
            'In this case client may take a decision... an something
            'For example... delete the row
            'TODO: Add fixture to test what happend if the client do nothing.
            e.Conflict.ServerChange.Rows(0).Item(0) = 5
        End If
        Me._EventRaisedOnInsertConflict = True
    End Sub

    ''' <summary>
    ''' 1-Server GetChanges.
    ''' 2-Client insert row N.
    ''' 3-Client update row N.
    ''' 4-Server GetChanges. This get change must return a rowstate equal to ADDED.
    ''' The update is transparent to the server.
    ''' </summary>
    ''' <param name="SyncDir"></param>
    ''' <remarks></remarks>
    Public Sub ClientInsertUpdate(ByVal SyncDir As SyncDirection)
        Dim RndGenerator As New Random
        Dim IdTestValue As Integer = RndGenerator.Next(1, 10)

        CreateSchema(SyncDir)

        Dim DataRetrieved As DataSet

        DataRetrieved = DoGetChanges(SyncDir)


        'First server GetChanges.
        Select Case SyncDir
            Case SyncDirection.Bidirectional, SyncDirection.UploadOnly
                Assert.AreEqual(1, DataRetrieved.Tables.Count)
                Assert.AreEqual(0, DataRetrieved.Tables(Me._SyncTable.TableName).Rows.Count)
            Case SyncDirection.DownloadOnly, SyncDirection.Snapshot
                Assert.AreEqual(0, DataRetrieved.Tables.Count)
        End Select

        Using _
            CMDInsert As _
                New SQLiteCommand("INSERT INTO TestTable(TestTable_ID,TestTable_Date) values(@Id, @Date)", _
                                   Me._Connection)
            CMDInsert.Parameters.Add("@Id", DbType.Int32).Value = IdTestValue
            CMDInsert.Parameters.Add("@Date", DbType.DateTime).Value = DateTime.Now
            CMDInsert.ExecuteNonQuery()
        End Using

        Using _
            CMDUpdate As _
                New SQLiteCommand("UPDATE TestTable SET TestTable_Date = @Date WHERE TestTable_Id = @Id", _
                                   Me._Connection)
            CMDUpdate.Parameters.Add("@Id", DbType.Int32).Value = IdTestValue
            CMDUpdate.Parameters.Add("@Date", DbType.DateTime).Value = DateTime.Now.AddDays(1)
            CMDUpdate.ExecuteNonQuery()
        End Using


        'Second server getchanges.
        DataRetrieved = DoGetChanges(SyncDir)

        'Asserts for second GetChanges
        Select Case SyncDir
            Case SyncDirection.Bidirectional, SyncDirection.UploadOnly
                Assert.AreEqual(1, DataRetrieved.Tables.Count)
                Assert.AreEqual(1, DataRetrieved.Tables(Me._SyncTable.TableName).Rows.Count)
                Assert.AreEqual(IdTestValue, _
                                 CType(DataRetrieved.Tables(Me._SyncTable.TableName).Rows(0).Item(0), Integer))
                Assert.AreEqual(DataRowState.Added, DataRetrieved.Tables(Me._SyncTable.TableName).Rows(0).RowState)
            Case SyncDirection.Snapshot, SyncDirection.DownloadOnly
                Assert.AreEqual(0, DataRetrieved.Tables.Count)
        End Select
        NoChangesTest(SyncDir)
    End Sub

    <TestMethod()> _
    Public Sub ClientInsertUpdateSnapshot()
        ClientInsertUpdate(SyncDirection.Snapshot)
    End Sub
    <TestMethod()> _
    Public Sub ClientInsertUpdateBidirectional()
        ClientInsertUpdate(SyncDirection.Bidirectional)
    End Sub
    <TestMethod()> _
    Public Sub ClientInsertUpdateDownloadOnly()
        ClientInsertUpdate(SyncDirection.DownloadOnly)
    End Sub
    <TestMethod()> _
    Public Sub ClientInsertUpdateUploadOnly()
        ClientInsertUpdate(SyncDirection.UploadOnly)
    End Sub

    ''' <summary>
    ''' 1-Client Insert. 2-Server GetChanges. 3-Client Update. 4-Server GetChanges
    ''' </summary>
    ''' <param name="SyncDir"></param>
    ''' <remarks></remarks>
    Public Sub ClientUpdateTest(ByVal SyncDir As SyncDirection)
        '1 and 2. Insert and sync.
        Dim IdTestValue As Integer = InsertRecordAndSync(SyncDir)


        '3- Client Update
        Using _
            CMDUpdate As _
                New SQLiteCommand("UPDATE TestTable SET TestTable_Date = @Date WHERE TestTable_Id = @Id", _
                                   Me._Connection)
            CMDUpdate.Parameters.Add("@Id", DbType.Int32).Value = IdTestValue
            CMDUpdate.Parameters.Add("@Date", DbType.DateTime).Value = DateTime.Now.AddDays(1)
            CMDUpdate.ExecuteNonQuery()
        End Using

        '4- Server GetChanges
        Dim DataRetrieved As DataSet = DoGetChanges(SyncDir)

        Select Case SyncDir
            Case SyncDirection.Bidirectional, SyncDirection.UploadOnly
                Assert.AreEqual(1, DataRetrieved.Tables.Count)
                Assert.AreEqual(1, DataRetrieved.Tables(Me._SyncTable.TableName).Rows.Count)
                Assert.AreEqual(IdTestValue, _
                                 CType(DataRetrieved.Tables(Me._SyncTable.TableName).Rows(0).Item(0), Integer))
                Assert.AreEqual(DataRowState.Modified, DataRetrieved.Tables(Me._SyncTable.TableName).Rows(0).RowState)
            Case SyncDirection.Snapshot, SyncDirection.DownloadOnly
                Assert.AreEqual(0, DataRetrieved.Tables.Count)
        End Select

        NoChangesTest(SyncDir)
    End Sub

    <TestMethod()> _
    Public Sub ClientUpdateTestSnapshot()
        ClientUpdateTest(SyncDirection.Snapshot)
    End Sub
    <TestMethod()> _
    Public Sub ClientUpdateTestBidirectional()
        ClientUpdateTest(SyncDirection.Bidirectional)
    End Sub
    <TestMethod()> _
    Public Sub ClientUpdateTestDownloadOnly()
        ClientUpdateTest(SyncDirection.DownloadOnly)
    End Sub
    <TestMethod()> _
    Public Sub ClientUpdateTestUploadOnly()
        ClientUpdateTest(SyncDirection.UploadOnly)
    End Sub

    ''' <summary>
    ''' 1-Client Insert. 
    ''' 2-Server GetChanges. 
    ''' 3-Client Delete. 
    ''' 4-Server GetChanges
    ''' </summary>
    ''' <param name="SyncDir"></param>
    ''' <remarks></remarks>
    Public Sub ClientDeleteTest(ByVal SyncDir As SyncDirection)
        '1-2. insert and sync.
        Dim IdTestValue As Integer = InsertRecordAndSync(SyncDir)

        '3- Client Delete
        Using CMDUpdate As New SQLiteCommand("DELETE FROM TestTable WHERE TestTable_Id = @Id", Me._Connection)
            CMDUpdate.Parameters.Add("@Id", DbType.Int32).Value = IdTestValue
            CMDUpdate.ExecuteNonQuery()
        End Using

        '4- Server GetChanges
        Dim DataRetrieved As DataSet = DoGetChanges(SyncDir)

        Select Case SyncDir
            Case SyncDirection.Bidirectional, SyncDirection.UploadOnly
                Assert.AreEqual(1, DataRetrieved.Tables.Count)
                Assert.AreEqual(1, DataRetrieved.Tables(Me._SyncTable.TableName).Rows.Count)
                Assert.AreEqual(IdTestValue, _
                                 CType( _
                                    DataRetrieved.Tables(Me._SyncTable.TableName).Rows(0).Item(0, _
                                                                                                  DataRowVersion. _
                                                                                                     Original), Integer))
                Assert.AreEqual(DataRowState.Deleted, DataRetrieved.Tables(Me._SyncTable.TableName).Rows(0).RowState)
            Case SyncDirection.Snapshot, SyncDirection.DownloadOnly
                Assert.AreEqual(0, DataRetrieved.Tables.Count)
        End Select

        NoChangesTest(SyncDir)
    End Sub

    <TestMethod()> _
    Public Sub ClientDeleteTestSnapshot()
        ClientUpdateTest(SyncDirection.Snapshot)
    End Sub
    <TestMethod()> _
    Public Sub ClientDeleteTestBidirectional()
        ClientUpdateTest(SyncDirection.Bidirectional)
    End Sub
    <TestMethod()> _
    Public Sub ClientDeleteTestDownloadOnly()
        ClientUpdateTest(SyncDirection.DownloadOnly)
    End Sub
    <TestMethod()> _
    Public Sub ClientDeleteTestUploadOnly()
        ClientUpdateTest(SyncDirection.UploadOnly)
    End Sub

    ''' <summary>
    ''' 1-Server GetChanges. 2-Client insert row N. 3-Client delete row N. 4-Server GetChanges.
    ''' </summary>
    ''' <param name="SyncDir"></param>
    ''' <remarks></remarks>
    Public Sub ClientInsertDeleteTest(ByVal SyncDir As SyncDirection)
        Dim RndGenerator As New Random
        Dim IdTestValue As Integer = RndGenerator.Next(1, 10)

        CreateSchema(SyncDir)

        Dim DataRetrieved As DataSet

        DataRetrieved = DoGetChanges(SyncDir)


        'First server GetChanges.
        Select Case SyncDir
            Case SyncDirection.Bidirectional, SyncDirection.UploadOnly
                Assert.AreEqual(1, DataRetrieved.Tables.Count)
                Assert.AreEqual(0, DataRetrieved.Tables(Me._SyncTable.TableName).Rows.Count)
            Case SyncDirection.DownloadOnly, SyncDirection.Snapshot
                Assert.AreEqual(0, DataRetrieved.Tables.Count)
        End Select

        Using _
            CMDInsert As _
                New SQLiteCommand("INSERT INTO TestTable(TestTable_ID,TestTable_Date) values(@Id, @Date)", _
                                   Me._Connection)
            CMDInsert.Parameters.Add("@Id", DbType.Int32).Value = IdTestValue
            CMDInsert.Parameters.Add("@Date", DbType.DateTime).Value = DateTime.Now
            CMDInsert.ExecuteNonQuery()
        End Using

        Using CMDUpdate As New SQLiteCommand("DELETE FROM TestTable WHERE TestTable_Id = @Id", Me._Connection)
            CMDUpdate.Parameters.Add("@Id", DbType.Int32).Value = IdTestValue
            CMDUpdate.ExecuteNonQuery()
        End Using


        'Second server getchanges.
        DataRetrieved = DoGetChanges(SyncDir)

        'Asserts for second GetChanges
        Select Case SyncDir
            Case SyncDirection.Bidirectional, SyncDirection.UploadOnly
                Assert.AreEqual(1, DataRetrieved.Tables.Count)
                Assert.AreEqual(0, DataRetrieved.Tables(Me._SyncTable.TableName).Rows.Count)
            Case SyncDirection.Snapshot, SyncDirection.DownloadOnly
                Assert.AreEqual(0, DataRetrieved.Tables.Count)
        End Select
        NoChangesTest(SyncDir)
    End Sub

    <TestMethod()> _
    Public Sub ClientInsertDeleteTestSnapshot()
        ClientInsertDeleteTest(SyncDirection.Snapshot)
    End Sub
    <TestMethod()> _
    Public Sub ClientInsertDeleteTestBidirectional()
        ClientInsertDeleteTest(SyncDirection.Bidirectional)
    End Sub
    <TestMethod()> _
    Public Sub ClientInsertDeleteTestDownloadOnly()
        ClientInsertDeleteTest(SyncDirection.DownloadOnly)
    End Sub
    <TestMethod()> _
    Public Sub ClientInsertDeleteTestUploadOnly()
        ClientInsertDeleteTest(SyncDirection.UploadOnly)
    End Sub


    ''' <summary>
    ''' 1-Client Insert. 2-Server Update. 3-Server apply update.
    ''' </summary>
    ''' <param name="SyncDir"></param>
    ''' <remarks></remarks>
    Public Sub ClientInsertServerUpdateTest(ByVal SyncDir As SyncDirection)
        Dim RndGenerator As New Random
        Dim IdTestValue As Integer = RndGenerator.Next(1, 10)

        CreateSchema(SyncDir)

        Dim DataRetrieved As DataSet

        'First server GetChanges.
        DataRetrieved = DoGetChanges(SyncDir)

        Select Case SyncDir
            Case SyncDirection.Bidirectional, SyncDirection.UploadOnly
                Assert.AreEqual(1, DataRetrieved.Tables.Count)
                Assert.AreEqual(0, DataRetrieved.Tables(Me._SyncTable.TableName).Rows.Count)
            Case SyncDirection.DownloadOnly, SyncDirection.Snapshot
                Assert.AreEqual(0, DataRetrieved.Tables.Count)
        End Select

        'Client Insert
        Using _
            CMDInsert As _
                New SQLiteCommand("INSERT INTO TestTable(TestTable_ID,TestTable_Date) values(@Id, @Date)", _
                                   Me._Connection)
            CMDInsert.Parameters.Add("@Id", DbType.Int32).Value = IdTestValue
            CMDInsert.Parameters.Add("@Date", DbType.DateTime).Value = DateTime.Now
            CMDInsert.ExecuteNonQuery()
        End Using

        'Second server getchanges.
        DataRetrieved = DoGetChanges(SyncDir)

        'Asserts for second GetChanges
        Select Case SyncDir
            Case SyncDirection.Bidirectional, SyncDirection.UploadOnly
                Assert.AreEqual(1, DataRetrieved.Tables.Count)
                Assert.AreEqual(1, DataRetrieved.Tables(Me._SyncTable.TableName).Rows.Count)
                Assert.AreEqual(IdTestValue, _
                                 CType(DataRetrieved.Tables(Me._SyncTable.TableName).Rows(0).Item(0), Integer))
                Assert.AreEqual(DataRowState.Added, DataRetrieved.Tables(Me._SyncTable.TableName).Rows(0).RowState)
            Case SyncDirection.Snapshot, SyncDirection.DownloadOnly
                Assert.AreEqual(0, DataRetrieved.Tables.Count)
        End Select


        'update row.
        Dim NewDate As DateTime = DateTime.Now
        Dim UpdateData As DataSet = GetDatasetToSend(IdTestValue, NewDate)
        UpdateData.AcceptChanges()
        UpdateData.Tables(0).Rows(0).SetModified()

        Dim SyncContext As SyncContext = DoApplyChanges(UpdateData)


        If Not SyncDir = SyncDirection.UploadOnly Then
            Assert.AreEqual(1, SyncContext.GroupProgress.TotalChanges)
            Assert.AreEqual(1, SyncContext.GroupProgress.TotalChangesApplied)
            Assert.AreEqual(0, SyncContext.GroupProgress.TotalChangesPending)
            Assert.AreEqual(0, SyncContext.GroupProgress.TotalDeletes)
            Assert.AreEqual(0, SyncContext.GroupProgress.TotalInserts)
            Assert.AreEqual(1, SyncContext.GroupProgress.TotalUpdates)

            Using _
                CountCommand As _
                    New SQLiteCommand( _
                                       "SELECT count(*) FROM TestTable WHERE TestTable_Id = @Id and TestTable_Date = @Date", _
                                       Me._Connection)
                CountCommand.Parameters.Add("@Id", DbType.Int32).Value = IdTestValue
                CountCommand.Parameters.Add("@Date", DbType.DateTime).Value = NewDate
                Assert.AreEqual(1, CType(CountCommand.ExecuteScalar, Integer))
            End Using
        End If


        NoChangesTest(SyncDir)
    End Sub

    <TestMethod()> _
    Public Sub ClientInsertServerUpdateTestBidirectional()
        ClientInsertServerUpdateTest(SyncDirection.Bidirectional)
    End Sub
    <TestMethod()> _
    Public Sub ClientInsertServerUpdateTestDownloadOnly()
        ClientInsertServerUpdateTest(SyncDirection.DownloadOnly)
    End Sub
    <TestMethod()> _
    Public Sub ClientInsertServerUpdateTestUploadOnly()
        ClientInsertServerUpdateTest(SyncDirection.UploadOnly)
    End Sub


    ''' <summary>
    ''' 1-Client Insert. 2-Server delete. 3-Server apply delete.
    ''' </summary>
    ''' <param name="SyncDir"></param>
    ''' <remarks></remarks>
    Public Sub ClientInsertServerDeleteTest(ByVal SyncDir As SyncDirection)
        Dim RndGenerator As New Random
        Dim IdTestValue As Integer = RndGenerator.Next(1, 10)

        CreateSchema(SyncDir)

        Dim DataRetrieved As DataSet

        'First server GetChanges.
        DataRetrieved = DoGetChanges(SyncDir)

        Select Case SyncDir
            Case SyncDirection.Bidirectional, SyncDirection.UploadOnly
                Assert.AreEqual(1, DataRetrieved.Tables.Count)
                Assert.AreEqual(0, DataRetrieved.Tables(Me._SyncTable.TableName).Rows.Count)
            Case SyncDirection.DownloadOnly, SyncDirection.Snapshot
                Assert.AreEqual(0, DataRetrieved.Tables.Count)
        End Select

        'Client Insert
        Using _
            CMDInsert As _
                New SQLiteCommand("INSERT INTO TestTable(TestTable_ID,TestTable_Date) values(@Id, @Date)", _
                                   Me._Connection)
            CMDInsert.Parameters.Add("@Id", DbType.Int32).Value = IdTestValue
            CMDInsert.Parameters.Add("@Date", DbType.DateTime).Value = DateTime.Now
            CMDInsert.ExecuteNonQuery()
        End Using

        'Second server getchanges.
        DataRetrieved = DoGetChanges(SyncDir)

        'Asserts for second GetChanges
        Select Case SyncDir
            Case SyncDirection.Bidirectional, SyncDirection.UploadOnly
                Assert.AreEqual(1, DataRetrieved.Tables.Count)
                Assert.AreEqual(1, DataRetrieved.Tables(Me._SyncTable.TableName).Rows.Count)
                Assert.AreEqual(IdTestValue, _
                                 CType(DataRetrieved.Tables(Me._SyncTable.TableName).Rows(0).Item(0), Integer))
                Assert.AreEqual(DataRowState.Added, DataRetrieved.Tables(Me._SyncTable.TableName).Rows(0).RowState)
            Case SyncDirection.Snapshot, SyncDirection.DownloadOnly
                Assert.AreEqual(0, DataRetrieved.Tables.Count)
        End Select


        'update row.
        Dim UpdateData As DataSet = GetDatasetToSend(IdTestValue, DateTime.Now)
        UpdateData.AcceptChanges()
        UpdateData.Tables(0).Rows(0).Delete()

        Dim SyncContext As SyncContext = DoApplyChanges(UpdateData)

        If Not SyncDir = SyncDirection.UploadOnly Then

            Assert.AreEqual(1, SyncContext.GroupProgress.TotalChanges)
            Assert.AreEqual(1, SyncContext.GroupProgress.TotalChangesApplied)
            Assert.AreEqual(0, SyncContext.GroupProgress.TotalChangesPending)
            Assert.AreEqual(1, SyncContext.GroupProgress.TotalDeletes)
            Assert.AreEqual(0, SyncContext.GroupProgress.TotalInserts)
            Assert.AreEqual(0, SyncContext.GroupProgress.TotalUpdates)
        Else
            Assert.AreEqual(1, GetActualRecordCount(IdTestValue))
        End If


        NoChangesTest(SyncDir)
    End Sub

    <TestMethod()> _
    Public Sub ClientInsertServerDeleteTestBidirectional()
        ClientInsertServerDeleteTest(SyncDirection.Bidirectional)
    End Sub
    <TestMethod()> _
    Public Sub ClientInsertServerDeleteTestDownloadOnly()
        ClientInsertServerDeleteTest(SyncDirection.DownloadOnly)
    End Sub
    <TestMethod()> _
    Public Sub ClientInsertServerDeleteTestUploadOnly()
        ClientInsertServerDeleteTest(SyncDirection.UploadOnly)
    End Sub

    ''' <summary>
    ''' Client update a row and server delete the same row before getchanges.
    ''' </summary>
    ''' <param name="SyncDir"></param>
    ''' <param name="ApplyAction"></param>
    ''' <remarks></remarks>
    Public Sub ServerDeleteClientUpdateConflictTest(ByVal SyncDir As SyncDirection, ByVal ApplyAction As ApplyAction)
        Dim IdClientInsert As Integer = InsertRecordAndSync(SyncDir)

        'Client update
        Using _
            CMD As _
                New SQLiteCommand("UPDATE TestTable SET TestTable_Date = @DateValue WHERE TestTable_Id = @Id", _
                                   Me._Connection)
            CMD.Parameters.Add("@Id", DbType.Int32).Value = IdClientInsert
            CMD.Parameters.Add("@DateValue", DbType.DateTime).Value = DateTime.Now
            CMD.ExecuteNonQuery()
        End Using

        'server update the row.
        Dim ServerDataset As DataSet = GetDatasetToSend(IdClientInsert, DateTime.Now)
        ServerDataset.AcceptChanges()
        ServerDataset.Tables(0).Rows(0).Delete()

        Me._ResoluciontAction = ApplyAction
        Me._EventRaisedOnDeleteConflict = False


        'Apply Changes.
        AddHandler _SqliteClientSyncProvider.ApplyChangeFailed, AddressOf OnApplyChangeDeleteConflict
        Dim SyncContext As SyncContext = DoApplyChanges(ServerDataset)


        'The default Conflict Resolver action, is CLIENT WINS. 
        'Therefore the event ApplyChangeFailed will not be fired and next GetChanges will return an update.
        Assert.IsFalse(Me._EventRaisedOnDeleteConflict)


        If Not SyncDir = SyncDirection.Bidirectional Then Exit Sub

        Assert.AreEqual(1, SyncContext.GroupProgress.TotalChanges)
        Assert.AreEqual(1, SyncContext.GroupProgress.TotalChangesFailed)
        Assert.AreEqual(0, SyncContext.GroupProgress.TotalChangesApplied)
        Assert.AreEqual(0, SyncContext.GroupProgress.TotalChangesPending)
        Assert.AreEqual(1, SyncContext.GroupProgress.TotalDeletes)
        Assert.AreEqual(0, SyncContext.GroupProgress.TotalInserts)
        Assert.AreEqual(0, SyncContext.GroupProgress.TotalUpdates)

        Assert.AreEqual(1, GetActualRecordCount())

        If SyncDir = SyncDirection.Bidirectional Then
            Dim DataRetrieved As DataSet = DoGetChanges(SyncDir)
            Assert.AreEqual(1, DataRetrieved.Tables.Count)
            Assert.AreEqual(1, DataRetrieved.Tables(0).Rows.Count)
            Assert.AreEqual(DataRowState.Modified, DataRetrieved.Tables(0).Rows(0).RowState)
        End If
        NoChangesTest(SyncDir)
    End Sub

    Public Sub OnApplyChangeDeleteConflict(ByVal sender As Object, ByVal e As ApplyChangeFailedEventArgs)
        Assert.AreEqual(ConflictType.ClientInsertServerInsert, e.Conflict.ConflictType)
        e.Action = Me._ResoluciontAction
        If e.Action = ApplyAction.RetryApplyingRow Then
            e.Conflict.ServerChange.Rows(0).AcceptChanges()
            e.Conflict.ServerChange.Rows.Remove(e.Conflict.ServerChange.Rows(0))
        End If
        Me._EventRaisedOnInsertConflict = True
    End Sub

    <TestMethod()> _
    Public Sub ServerDeleteClientUpdateConflictTest_Snapshot_Continue()
        ServerDeleteClientUpdateConflictTest(SyncDirection.Snapshot, ApplyAction.Continue)
    End Sub
    <TestMethod()> _
    Public Sub ServerDeleteClientUpdateConflictTest_Snapshot_RetryApplyingRow()
        ServerDeleteClientUpdateConflictTest(SyncDirection.Snapshot, ApplyAction.RetryApplyingRow)
    End Sub
    <TestMethod()> _
    Public Sub ServerDeleteClientUpdateConflictTest_Snapshot_RetryWithForceWrite()
        ServerDeleteClientUpdateConflictTest(SyncDirection.Snapshot, ApplyAction.RetryWithForceWrite)
    End Sub

    <TestMethod()> _
    Public Sub ServerDeleteClientUpdateConflictTest_Bidirectional_Continue()
        ServerDeleteClientUpdateConflictTest(SyncDirection.Bidirectional, ApplyAction.Continue)
    End Sub
    <TestMethod()> _
    Public Sub ServerDeleteClientUpdateConflictTest_Bidirectional_RetryApplyingRow()
        ServerDeleteClientUpdateConflictTest(SyncDirection.Bidirectional, ApplyAction.RetryApplyingRow)
    End Sub
    <TestMethod()> _
    Public Sub ServerDeleteClientUpdateConflictTest_Bidirectional_RetryWithForceWrite()
        ServerDeleteClientUpdateConflictTest(SyncDirection.Bidirectional, ApplyAction.RetryWithForceWrite)
    End Sub

    <TestMethod()> _
    Public Sub ServerDeleteClientUpdateConflictTest_DownloadOnly_Continue()
        ServerDeleteClientUpdateConflictTest(SyncDirection.DownloadOnly, ApplyAction.Continue)
    End Sub
    <TestMethod()> _
    Public Sub ServerDeleteClientUpdateConflictTest_DownloadOnly_RetryApplyingRow()
        ServerDeleteClientUpdateConflictTest(SyncDirection.DownloadOnly, ApplyAction.RetryApplyingRow)
    End Sub
    <TestMethod()> _
    Public Sub ServerDeleteClientUpdateConflictTest_DownloadOnly_RetryWithForceWrite()
        ServerDeleteClientUpdateConflictTest(SyncDirection.DownloadOnly, ApplyAction.RetryWithForceWrite)
    End Sub

    <TestMethod()> _
    Public Sub ServerDeleteClientUpdateConflictTest_UploadOnly_Continue()
        ServerDeleteClientUpdateConflictTest(SyncDirection.UploadOnly, ApplyAction.Continue)
    End Sub
    <TestMethod()> _
    Public Sub ServerDeleteClientUpdateConflictTest_UploadOnly_RetryApplyingRow()
        ServerDeleteClientUpdateConflictTest(SyncDirection.UploadOnly, ApplyAction.RetryApplyingRow)
    End Sub
    <TestMethod()> _
    Public Sub ServerDeleteClientUpdateConflictTest_UploadOnly_RetryWithForceWrite()
        ServerDeleteClientUpdateConflictTest(SyncDirection.UploadOnly, ApplyAction.RetryWithForceWrite)
    End Sub



    Public Sub ServerUpdateClientDeleteConflictTest(ByVal SyncDir As SyncDirection, ByVal ApplyAction As ApplyAction)
        Dim IdClientInsert As Integer = InsertRecordAndSync(SyncDir)

        'Client delete
        Using CMD As New SQLiteCommand("DELETE FROM TestTable WHERE TestTable_Id = @Id", Me._Connection)
            CMD.Parameters.Add("@Id", DbType.Int32).Value = IdClientInsert
            CMD.ExecuteNonQuery()
        End Using

        'Server update the row.
        Dim ServerDataset As DataSet = GetDatasetToSend(IdClientInsert, DateTime.Now)
        ServerDataset.AcceptChanges()
        ServerDataset.Tables(0).Rows(0).SetModified()


        Me._ResoluciontAction = ApplyAction
        Me._EventRaisedOnUpdateConflict = False


        'Apply Changes.
        AddHandler _SqliteClientSyncProvider.ApplyChangeFailed, AddressOf OnApplyChangeSUCDConflict
        Dim SyncContext As SyncContext = DoApplyChanges(ServerDataset)


        'The default Conflict Resolver action, is SERVER WINS = RetryWithForceWrite.
        'Therefore the event ApplyChangeFailed will not be fired and next GetChanges will return an update.
        Assert.IsFalse(Me._EventRaisedOnUpdateConflict)


        If Not SyncDir = SyncDirection.Bidirectional Then Exit Sub

        Assert.AreEqual(1, SyncContext.GroupProgress.TotalChanges)
        Assert.AreEqual(0, SyncContext.GroupProgress.TotalChangesFailed)
        Assert.AreEqual(1, SyncContext.GroupProgress.TotalChangesApplied)
        Assert.AreEqual(0, SyncContext.GroupProgress.TotalChangesPending)
        Assert.AreEqual(0, SyncContext.GroupProgress.TotalDeletes)
        Assert.AreEqual(0, SyncContext.GroupProgress.TotalInserts)
        Assert.AreEqual(1, SyncContext.GroupProgress.TotalUpdates)

        Assert.AreEqual(1, GetActualRecordCount())


        NoChangesTest(SyncDir)
    End Sub

    <TestMethod()> _
    Public Sub ServerUpdateClientDeleteConflictTest_Snapshot_Continue()
        ServerUpdateClientDeleteConflictTest(SyncDirection.Snapshot, ApplyAction.Continue)
    End Sub
    <TestMethod()> _
    Public Sub ServerUpdateClientDeleteConflictTest_Snapshot_RetryApplyingRow()
        ServerUpdateClientDeleteConflictTest(SyncDirection.Snapshot, ApplyAction.RetryApplyingRow)
    End Sub
    <TestMethod()> _
    Public Sub ServerUpdateClientDeleteConflictTest_Snapshot_RetryWithForceWrite()
        ServerUpdateClientDeleteConflictTest(SyncDirection.Snapshot, ApplyAction.RetryWithForceWrite)
    End Sub

    <TestMethod()> _
    Public Sub ServerUpdateClientDeleteConflictTest_Bidirectional_Continue()
        ServerUpdateClientDeleteConflictTest(SyncDirection.Bidirectional, ApplyAction.Continue)
    End Sub
    <TestMethod()> _
    Public Sub ServerUpdateClientDeleteConflictTest_Bidirectional_RetryApplyingRow()
        ServerUpdateClientDeleteConflictTest(SyncDirection.Bidirectional, ApplyAction.RetryApplyingRow)
    End Sub
    <TestMethod()> _
    Public Sub ServerUpdateClientDeleteConflictTest_Bidirectional_RetryWithForceWrite()
        ServerUpdateClientDeleteConflictTest(SyncDirection.Bidirectional, ApplyAction.RetryWithForceWrite)
    End Sub

    <TestMethod()> _
    Public Sub ServerUpdateClientDeleteConflictTest_DownloadOnly_Continue()
        ServerUpdateClientDeleteConflictTest(SyncDirection.DownloadOnly, ApplyAction.Continue)
    End Sub
    <TestMethod()> _
    Public Sub ServerUpdateClientDeleteConflictTest_DownloadOnly_RetryApplyingRow()
        ServerUpdateClientDeleteConflictTest(SyncDirection.DownloadOnly, ApplyAction.RetryApplyingRow)
    End Sub
    <TestMethod()> _
    Public Sub ServerUpdateClientDeleteConflictTest_DownloadOnly_RetryWithForceWrite()
        ServerUpdateClientDeleteConflictTest(SyncDirection.DownloadOnly, ApplyAction.RetryWithForceWrite)
    End Sub

    <TestMethod()> _
    Public Sub ServerUpdateClientDeleteConflictTest_UploadOnly_Continue()
        ServerUpdateClientDeleteConflictTest(SyncDirection.UploadOnly, ApplyAction.Continue)
    End Sub
    <TestMethod()> _
    Public Sub ServerUpdateClientDeleteConflictTest_UploadOnly_RetryApplyingRow()
        ServerUpdateClientDeleteConflictTest(SyncDirection.UploadOnly, ApplyAction.RetryApplyingRow)
    End Sub
    <TestMethod()> _
    Public Sub ServerUpdateClientDeleteConflictTest_UploadOnly_RetryWithForceWrite()
        ServerUpdateClientDeleteConflictTest(SyncDirection.UploadOnly, ApplyAction.RetryWithForceWrite)
    End Sub

    ''' <summary>
    ''' Server update client delete conflict event.
    ''' </summary>
    ''' <param name="sender"></param>
    ''' <param name="e"></param>
    ''' <remarks></remarks>
    Public Sub OnApplyChangeSUCDConflict(ByVal sender As Object, ByVal e As ApplyChangeFailedEventArgs)
        Assert.AreEqual(ConflictType.ClientInsertServerInsert, e.Conflict.ConflictType)
        e.Action = Me._ResoluciontAction
        If e.Action = ApplyAction.RetryApplyingRow Then
            e.Conflict.ServerChange.Rows(0).AcceptChanges()
            e.Conflict.ServerChange.Rows.Remove(e.Conflict.ServerChange.Rows(0))
        End If
        Me._EventRaisedOnInsertConflict = True
    End Sub


    ''' <summary>
    ''' Client and server update the same row.
    ''' </summary>
    ''' <param name="SyncDir"></param>
    ''' <param name="ApplyAction"></param>
    ''' <remarks></remarks>
    Public Sub ServerUpdateClientUpdateConflictTest(ByVal SyncDir As SyncDirection, ByVal ApplyAction As ApplyAction)
        Dim IdClientInsert As Integer = InsertRecordAndSync(SyncDir)

        'Client update
        Using _
            CMD As _
                New SQLiteCommand("UPDATE TestTable SET TestTable_Date = @Date WHERE TestTable_Id = @Id", _
                                   Me._Connection)
            CMD.Parameters.Add("@Id", DbType.Int32).Value = IdClientInsert
            CMD.Parameters.Add("@Date", DbType.DateTime).Value = DateTime.Now
            CMD.ExecuteNonQuery()
        End Using

        'Server update the row.
        Dim ServerDataset As DataSet = GetDatasetToSend(IdClientInsert, DateTime.Now)
        ServerDataset.AcceptChanges()
        ServerDataset.Tables(0).Rows(0).SetModified()


        Me._ResoluciontAction = ApplyAction
        Me._EventRaisedOnUpdateConflict = False


        'Apply Changes.
        AddHandler _SqliteClientSyncProvider.ApplyChangeFailed, AddressOf OnApplyChangeSUCUConflict
        Dim SyncContext As SyncContext = DoApplyChanges(ServerDataset)


        'The default Conflict Resolver action, is FIRE EVENT.
        'Therefore the event ApplyChangeFailed will not be fired and next GetChanges will return an update.
        'Assert.AreEqual(SyncDir = SyncDirection.Bidirectional, Me._EventRaisedOnUpdateConflict)


        If Not SyncDir = SyncDirection.Bidirectional Then Exit Sub

        Assert.IsTrue(Me._EventRaisedOnUpdateConflict)

        If ApplyAction = Microsoft.Synchronization.Data.ApplyAction.Continue Then
            Assert.AreEqual(1, SyncContext.GroupProgress.TotalChanges)
            Assert.AreEqual(1, SyncContext.GroupProgress.TotalChangesFailed)
            Assert.AreEqual(0, SyncContext.GroupProgress.TotalChangesApplied)
            Assert.AreEqual(0, SyncContext.GroupProgress.TotalChangesPending)
            Assert.AreEqual(0, SyncContext.GroupProgress.TotalDeletes)
            Assert.AreEqual(0, SyncContext.GroupProgress.TotalInserts)
            Assert.AreEqual(1, SyncContext.GroupProgress.TotalUpdates)
        Else
            Assert.AreEqual(1, SyncContext.GroupProgress.TotalChanges)
            Assert.AreEqual(0, SyncContext.GroupProgress.TotalChangesFailed)
            Assert.AreEqual(1, SyncContext.GroupProgress.TotalChangesApplied)
            Assert.AreEqual(0, SyncContext.GroupProgress.TotalChangesPending)
            Assert.AreEqual(0, SyncContext.GroupProgress.TotalDeletes)
            Assert.AreEqual(0, SyncContext.GroupProgress.TotalInserts)
            Assert.AreEqual(1, SyncContext.GroupProgress.TotalUpdates)
        End If


        Assert.AreEqual(1, GetActualRecordCount())

        DoGetChanges(SyncDir)

        NoChangesTest(SyncDir)
    End Sub

    <TestMethod()> _
    Public Sub ServerUpdateClientUpdateConflictTest_Snapshot_Continue()
        ServerUpdateClientUpdateConflictTest(SyncDirection.Snapshot, ApplyAction.Continue)
    End Sub
    <TestMethod()> _
    Public Sub ServerUpdateClientUpdateConflictTest_Snapshot_RetryApplyingRow()
        ServerUpdateClientUpdateConflictTest(SyncDirection.Snapshot, ApplyAction.RetryApplyingRow)
    End Sub
    <TestMethod()> _
    Public Sub ServerUpdateClientUpdateConflictTest_Snapshot_RetryWithForceWrite()
        ServerUpdateClientUpdateConflictTest(SyncDirection.Snapshot, ApplyAction.RetryWithForceWrite)
    End Sub

    <TestMethod()> _
    Public Sub ServerUpdateClientUpdateConflictTest_Bidirectional_Continue()
        ServerUpdateClientUpdateConflictTest(SyncDirection.Bidirectional, ApplyAction.Continue)
    End Sub
    <TestMethod()> _
    Public Sub ServerUpdateClientUpdateConflictTest_Bidirectional_RetryApplyingRow()
        ServerUpdateClientUpdateConflictTest(SyncDirection.Bidirectional, ApplyAction.RetryApplyingRow)
    End Sub
    <TestMethod()> _
    Public Sub ServerUpdateClientUpdateConflictTest_Bidirectional_RetryWithForceWrite()
        ServerUpdateClientUpdateConflictTest(SyncDirection.Bidirectional, ApplyAction.RetryWithForceWrite)
    End Sub

    <TestMethod()> _
    Public Sub ServerUpdateClientUpdateConflictTest_UploadOnly_Continue()
        ServerUpdateClientUpdateConflictTest(SyncDirection.UploadOnly, ApplyAction.Continue)
    End Sub
    <TestMethod()> _
    Public Sub ServerUpdateClientUpdateConflictTest_UploadOnly_RetryApplyingRow()
        ServerUpdateClientUpdateConflictTest(SyncDirection.UploadOnly, ApplyAction.RetryApplyingRow)
    End Sub
    <TestMethod()> _
    Public Sub ServerUpdateClientUpdateConflictTest_UploadOnly_RetryWithForceWrite()
        ServerUpdateClientUpdateConflictTest(SyncDirection.UploadOnly, ApplyAction.RetryWithForceWrite)
    End Sub

    <TestMethod()> _
    Public Sub ServerUpdateClientUpdateConflictTest_DownloadOnly_Continue()
        ServerUpdateClientUpdateConflictTest(SyncDirection.DownloadOnly, ApplyAction.Continue)
    End Sub
    <TestMethod()> _
    Public Sub ServerUpdateClientUpdateConflictTest_DownloadOnly_RetryApplyingRow()
        ServerUpdateClientUpdateConflictTest(SyncDirection.DownloadOnly, ApplyAction.RetryApplyingRow)
    End Sub
    <TestMethod()> _
    Public Sub ServerUpdateClientUpdateConflictTest_DownloadOnly_RetryWithForceWrite()
        ServerUpdateClientUpdateConflictTest(SyncDirection.DownloadOnly, ApplyAction.RetryWithForceWrite)
    End Sub


    Public Sub OnApplyChangeSUCUConflict(ByVal sender As Object, ByVal e As ApplyChangeFailedEventArgs)
        Assert.AreEqual(ConflictType.ClientUpdateServerUpdate, e.Conflict.ConflictType)
        e.Action = Me._ResoluciontAction
        If e.Action = ApplyAction.RetryApplyingRow Then
            'do something with the server and client change.
            e.Conflict.ServerChange.Rows(0).Item(1) = DateTime.Now
        End If
        Me._EventRaisedOnUpdateConflict = True
    End Sub

    Private Function SerializeAnchorValue(ByVal anchorVal As Object) As Byte()
        Dim serializationStream As New MemoryStream()
        Dim BF As New BinaryFormatter

        BF.Serialize(serializationStream, anchorVal)

        Dim ret As Byte() = serializationStream.ToArray()

        serializationStream.Dispose()

        Return ret
    End Function

    Public Function DeserializeAnchorValue(ByVal anchor As Byte()) As Object

        If anchor Is Nothing Then Return Nothing

        Dim serializationStream As New MemoryStream(anchor)
        Dim BF As New BinaryFormatter

        Dim ret As Object = BF.Deserialize(serializationStream)

        serializationStream.Dispose()

        Return ret

    End Function
End Class
