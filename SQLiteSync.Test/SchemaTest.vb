Imports Microsoft.VisualStudio.TestTools.UnitTesting
Imports Microsoft.Synchronization.Data
Imports System.Data.SQLite
Imports System.Data
Imports System.Reflection

''' <summary>
''' Basic testfixture for sqlite client sync provider.
''' </summary>
''' <remarks></remarks>
<TestClass()> _
Public Class SchemaTest
    'Private _DBFile As String
    Private _Connection As SQLiteConnection
    Private _SqliteClientSyncProvider As SqliteClientSyncProvider
    Private _SyncSession As SyncSession

    Private AssemblyName As String = Assembly.GetExecutingAssembly.GetName.Name
    Private SyncTable As SyncTable = Serializator(Of SyncTable).LoadFromStream(Assembly.GetExecutingAssembly.GetManifestResourceStream(AssemblyName & ".test_SyncTable.xml"))  '.Load(GetPath() & "test_synctable.xml")
    Private SyncSchema As SyncSchema = Serializator(Of SyncSchema).LoadFromStream(Assembly.GetExecutingAssembly.GetManifestResourceStream(AssemblyName & ".test_SyncSchema.xml"))

    <TestInitialize()> _
    Public Sub Setup()
        '_DBFile = System.IO.Path.GetTempFileName
        '_ConnectionString = String.Format("Data source={0}", _DBFile)
        _Connection = New SQLiteConnection("Data source=:memory:")
        _Connection.Open()
        _SqliteClientSyncProvider = New SqliteClientSyncProvider(Me._Connection)
        _SyncSession = New SyncSession
        _SqliteClientSyncProvider.BeginTransaction(_SyncSession)
    End Sub

    <TestMethod()> _
    Public Sub GetNullSentAnchor()
        Assert.IsNull(Me._SqliteClientSyncProvider.GetTableSentAnchor("TestTable"))
    End Sub

    <TestMethod()> _
    Public Sub GetNullReceivedAnchor()
        Assert.IsNull(Me._SqliteClientSyncProvider.GetTableSentAnchor("TestTable"))
    End Sub

    Public Sub CreateSchemaTest(ByVal syncDir As SyncDirection)

        SyncTable.SyncDirection = syncDir

        'Table no exists before the test.
        Using cmdTableExists As New SQLiteCommand("SELECT count(*) " & _
                                                                         "FROM sqlite_master " & _
                                                                         "WHERE type = 'table' " & _
                                                                         "AND name = @TableName", Me._Connection)

            cmdTableExists.Parameters.Add("@TableName", DbType.String).Value = SyncTable.TableName
            Assert.AreEqual(0, CType(cmdTableExists.ExecuteScalar(), Integer), "Table not exists before create schema")
        End Using

        'Create schema
        Me._SqliteClientSyncProvider.CreateSchema(SyncTable, SyncSchema)
        Me._SqliteClientSyncProvider.EndTransaction(True, Me._SyncSession)

        'If table exists then sent anchor must not be null.
        Me._SqliteClientSyncProvider.BeginTransaction(Me._SyncSession)
        Assert.IsNotNull(Me._SqliteClientSyncProvider.GetTableSentAnchor(SyncTable.TableName))
        Me._SqliteClientSyncProvider.EndTransaction(True, Me._SyncSession)

        'Verify if table exists.
        Using cmdTableExists As New SQLiteCommand("SELECT count(*) " & _
                                                                     "FROM sqlite_master " & _
                                                                     "WHERE type = 'table' " & _
                                                                     "AND name = @TableName", Me._Connection)

            cmdTableExists.Parameters.Add("@TableName", DbType.String).Value = SyncTable.TableName
            Assert.AreEqual(1, CType(cmdTableExists.ExecuteScalar(), Integer), "Table exists after create schema")
        End Using

        'Verify if table cols are ok.
        Using cmdTableCols As New SQLiteCommand("SELECT * " & _
                                                "FROM " & SyncTable.TableName, Me._Connection)

            Using dr As SQLite.SQLiteDataReader = cmdTableCols.ExecuteReader(CommandBehavior.KeyInfo)
                Using dt As DataTable = dr.GetSchemaTable
                    For i As Integer = 0 To SyncSchema.Tables(SyncTable.TableName).Columns.Count - 1
                        Dim oCol As SyncSchemaColumn = SyncSchema.Tables(SyncTable.TableName).Columns(i)
                        Assert.IsTrue(dt.Select(String.Format("ColumnName = '{0}'", oCol.ColumnName)).Length > 0, String.Format("Table contains column {0}.", oCol.ColumnName))
                    Next
                    Select Case syncDir
                        Case SyncDirection.DownloadOnly, SyncDirection.Snapshot
                            Assert.IsTrue(dt.Select("ColumnName = '__sysChangeTxBsn'").Length = 0, "Table contains column __sysChangeTxBsn.")
                            Assert.IsTrue(dt.Select("ColumnName = '__sysChangeTxBsn'").Length = 0, "Table contains column __sysInsertTxBsn.")
                            Assert.IsTrue(dt.Select("ColumnName = '__sysReceived'").Length = 0, "Table not contains column __sysInsertTxBsn.")
                        Case SyncDirection.UploadOnly
                            Assert.IsTrue(dt.Select("ColumnName = '__sysChangeTxBsn'").Length = 1, "Table contains column __sysChangeTxBsn.")
                            Assert.IsTrue(dt.Select("ColumnName = '__sysChangeTxBsn'").Length = 1, "Table contains column __sysInsertTxBsn.")
                            Assert.IsTrue(dt.Select("ColumnName = '__sysReceived'").Length = 0, "Table not contains column __sysInsertTxBsn.")
                        Case SyncDirection.Bidirectional
                            Assert.IsTrue(dt.Select("ColumnName = '__sysChangeTxBsn'").Length = 1, "Table contains column __sysChangeTxBsn.")
                            Assert.IsTrue(dt.Select("ColumnName = '__sysChangeTxBsn'").Length = 1, "Table contains column __sysInsertTxBsn.")
                            Assert.IsTrue(dt.Select("ColumnName = '__sysReceived'").Length = 1, "Table not contains column __sysInsertTxBsn.")
                    End Select
                End Using
            End Using
        End Using

        'Verify if the three table triggers exists.
        Using cmdTriggerCount As New SQLiteCommand("SELECT count(*) " & _
                                                             "FROM sqlite_master " & _
                                                             "WHERE type = 'trigger' " & _
                                                            "AND name in ('ON_TBL_' || @TableName || '_UPDATE','ON_TBL_' || @TableName || '_DELETE','ON_TBL_' || @TableName || '_INSERT') ", Me._Connection)

            cmdTriggerCount.Parameters.Add("@TableName", DbType.String).Value = SyncTable.TableName

            Select Case syncDir
                Case SyncDirection.DownloadOnly, SyncDirection.Snapshot
                    Assert.AreEqual(0, CType(cmdTriggerCount.ExecuteScalar(), Integer), "The triggers were created.")
                Case SyncDirection.UploadOnly, SyncDirection.Bidirectional
                    Assert.AreEqual(3, CType(cmdTriggerCount.ExecuteScalar(), Integer), "The triggers were created.")
            End Select
        End Using
    End Sub

    <TestMethod()> _
    Public Sub CreateSchemaTest_Snapshot()
        CreateSchemaTest(SyncDirection.Snapshot)
    End Sub
    <TestMethod()> _
    Public Sub CreateSchemaTest_DownloadOnly()
        CreateSchemaTest(SyncDirection.DownloadOnly)
    End Sub
    <TestMethod()> _
    Public Sub CreateSchemaTest_UploadOnly()
        CreateSchemaTest(SyncDirection.UploadOnly)
    End Sub
    <TestMethod()> _
    Public Sub CreateSchemaTest_Bidirectional()
        CreateSchemaTest(SyncDirection.Bidirectional)
    End Sub

    <TestCleanup()> _
    Public Sub TearDown()
        If Me._Connection IsNot Nothing AndAlso Me._Connection.State = ConnectionState.Open Then
            Me._Connection.Close()
        End If
        '_Connection.Dispose() ' done by the sync provider
        _SqliteClientSyncProvider.Dispose()
        _SqliteClientSyncProvider = Nothing
        'System.IO.File.Delete(Me._DBFile)
    End Sub

End Class
