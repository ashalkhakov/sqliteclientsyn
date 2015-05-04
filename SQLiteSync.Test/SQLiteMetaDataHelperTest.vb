Imports Microsoft.VisualStudio.TestTools.UnitTesting
Imports System.Data
Imports SQLiteSync

<TestClass()> _
Public Class SQLiteMetaDataHelperTest
    Private _Connection As SQLite.SQLiteConnection
    Private _SQLiteMetaDataHelper As Tools.SQLiteMetaDataHelper

    <TestInitialize()> _
    Public Sub Setup()
        _Connection = New SQLite.SQLiteConnection("Data source=:memory:")
        _Connection.Open()
    End Sub

    <TestMethod()> _
    <TestProperty("Description", "Test if class could be instantiate with null arguments.")> _
    Public Sub TestInstantationNullArgs()
        Try
            _SQLiteMetaDataHelper = New Tools.SQLiteMetaDataHelper(Nothing, Nothing, True)
        Catch e As ArgumentException
            ' OK
        End Try
    End Sub

    <TestMethod()> _
    <TestProperty("Description", "Test if class could be instantiate with null transaction argument arguments.")> _
    Public Sub TestInstantationNullTx()
        Try
            _SQLiteMetaDataHelper = New Tools.SQLiteMetaDataHelper(Me._Connection, Nothing, True)
        Catch e As ArgumentException
            ' OK
        End Try
    End Sub

    Public Sub TestMetadataTableCreation(ByVal TableName As String)

        Using TX As System.Data.SQLite.SQLiteTransaction = Me._Connection.BeginTransaction
            _SQLiteMetaDataHelper = New Tools.SQLiteMetaDataHelper(Me._Connection, TX, True)

            Using cmdTableExists As New SQLite.SQLiteCommand("SELECT count(*) " & _
                                                             "FROM sqlite_master " & _
                                                             "WHERE type = 'table' " & _
                                                             "AND name = @TableName", Me._Connection, TX)

                cmdTableExists.Parameters.Add("@TableName", DbType.String).Value = TableName
                Assert.AreEqual(1, CType(cmdTableExists.ExecuteScalar(), Integer), "__Anchor was not created.")
            End Using

            TX.Commit()
        End Using

    End Sub

    <TestMethod()> _
    Public Sub TestMetadataTableCreation_Anchor()
        TestMetadataTableCreation("__Anchor")
    End Sub
    <TestMethod()> _
    Public Sub TestMetadataTableCreation_ClientGuid()
        TestMetadataTableCreation("__ClientGuid")
    End Sub

    Public Sub TestTableExistFunction(ByVal TableName As String)

        Using TX As System.Data.SQLite.SQLiteTransaction = Me._Connection.BeginTransaction
            _SQLiteMetaDataHelper = New Tools.SQLiteMetaDataHelper(Me._Connection, TX, True)
            Assert.IsTrue(Me._SQLiteMetaDataHelper.TableExists(TableName))
            TX.Commit()
        End Using

    End Sub

    <TestMethod()> _
    Public Sub TestTableExistFunction_Anchor()
        TestTableExistFunction("__Anchor")
    End Sub
    <TestMethod()> _
    Public Sub TestTableExistFunction_ClientGuid()
        TestTableExistFunction("__ClientGuid")
    End Sub

    Public Sub TestGetEmptyAnchor(ByVal TypeOfAnchor As Tools.AnchorType)

        Using TX As System.Data.SQLite.SQLiteTransaction = Me._Connection.BeginTransaction
            _SQLiteMetaDataHelper = New Tools.SQLiteMetaDataHelper(Me._Connection, TX, True)
            Assert.AreEqual(DBNull.Value, _SQLiteMetaDataHelper.GetAnchorValue("TestTable", TypeOfAnchor))
        End Using

    End Sub

    <TestMethod()> _
    Public Sub TestGetEmptyAnchor_SentAnchor()
        TestGetEmptyAnchor(Tools.AnchorType.SentAnchor)
    End Sub
    <TestMethod()> _
    Public Sub TestGetEmptyAnchor_ReceivedAnchor()
        TestGetEmptyAnchor(Tools.AnchorType.ReceivedAnchor)
    End Sub

    <TestMethod()> _
    Public Sub TestSetInvalidSentAnchor()
        Try

            Using TX As System.Data.SQLite.SQLiteTransaction = Me._Connection.BeginTransaction
                _SQLiteMetaDataHelper = New Tools.SQLiteMetaDataHelper(Me._Connection, TX, True)
                'Assert.AreEqual(DBNull.Value, _SQLiteMetaDataHelper.GetAnchorValue("TestTable", TypeOfAnchor))
                Dim byteData As Byte() = {0, 145, 12, 0, 23, 0, 0, 1}
                'Dim longData As Long = CType(213, Long)
                'Dim dateData As Date = Now
                _SQLiteMetaDataHelper.SetAnchorValue("TestTableByte", Tools.AnchorType.SentAnchor, byteData)
            End Using

        Catch e As ArgumentException
            ' OK
        End Try

    End Sub

    <TestMethod()> _
    Public Sub TestSetInvalidReceivedAnchor()
        Try

            Using TX As System.Data.SQLite.SQLiteTransaction = Me._Connection.BeginTransaction
                _SQLiteMetaDataHelper = New Tools.SQLiteMetaDataHelper(Me._Connection, TX, True)
                'Assert.AreEqual(DBNull.Value, _SQLiteMetaDataHelper.GetAnchorValue("TestTable", TypeOfAnchor))
                'Dim byteData As Byte() = {0, 145, 12, 0, 23, 0, 0, 1}
                'Dim longData As Long = CType(213, Long)
                Dim dateData As Date = Now
                _SQLiteMetaDataHelper.SetAnchorValue("TestTableByte", Tools.AnchorType.ReceivedAnchor, dateData)
            End Using

        Catch e As ArgumentException
            ' OK
        End Try

    End Sub

    <TestMethod()> _
    Public Sub TestSetGetReceivedAnchor()

        Using TX As System.Data.SQLite.SQLiteTransaction = Me._Connection.BeginTransaction
            _SQLiteMetaDataHelper = New Tools.SQLiteMetaDataHelper(Me._Connection, TX, True)
            'Assert.AreEqual(DBNull.Value, _SQLiteMetaDataHelper.GetAnchorValue("TestTable", TypeOfAnchor))
            Dim byteData As Byte() = {0, 145, 12, 0, 23, 0, 0, 1}
            Dim longData As Long = CType(213, Long)
            'Dim dateData As Date = Now

            _SQLiteMetaDataHelper.SetAnchorValue("TestTableByte", Tools.AnchorType.ReceivedAnchor, byteData)
            CollectionAssert.AreEqual(byteData, _SQLiteMetaDataHelper.GetAnchorValue("TestTableByte", Tools.AnchorType.ReceivedAnchor), "Invalid get received anchor value.")


            _SQLiteMetaDataHelper.SetAnchorValue("TestTableLong", Tools.AnchorType.ReceivedAnchor, longData)
            Assert.AreEqual(longData, _SQLiteMetaDataHelper.GetAnchorValue("TestTableLong", Tools.AnchorType.ReceivedAnchor), "Invalid get received anchor value.")
        End Using

    End Sub

    <TestMethod()> _
    Public Sub TestSetGetSentAnchor()

        Using TX As System.Data.SQLite.SQLiteTransaction = Me._Connection.BeginTransaction
            _SQLiteMetaDataHelper = New Tools.SQLiteMetaDataHelper(Me._Connection, TX, True)


            _SQLiteMetaDataHelper.SetAnchorValue("TestTableDate", Tools.AnchorType.SentAnchor, CType(1, Int64))
            Assert.AreEqual(CType(1, Int64), _SQLiteMetaDataHelper.GetAnchorValue("TestTableDate", Tools.AnchorType.SentAnchor), "Invalid get received anchor value.")

        End Using

    End Sub

    <TestMethod()> _
    Public Sub TestSetUpdateSentAnchor()

        Using TX As System.Data.SQLite.SQLiteTransaction = Me._Connection.BeginTransaction
            _SQLiteMetaDataHelper = New Tools.SQLiteMetaDataHelper(Me._Connection, TX, True)
            Dim value As Int64 = CType(0, Long)

            _SQLiteMetaDataHelper.SetAnchorValue("TestTableDate", Tools.AnchorType.SentAnchor, value)

            Dim newValue As Int64 = CType(1, Int64)
            _SQLiteMetaDataHelper.SetAnchorValue("TestTableDate", Tools.AnchorType.SentAnchor, newValue)
            Assert.AreEqual(newValue, _SQLiteMetaDataHelper.GetAnchorValue("TestTableDate", Tools.AnchorType.SentAnchor), "Invalid get received anchor value.")

        End Using

    End Sub


    <TestMethod()> _
    Public Sub TestSetInvalidTypeSentAnchor()
        Try

            Using TX As System.Data.SQLite.SQLiteTransaction = Me._Connection.BeginTransaction
                _SQLiteMetaDataHelper = New Tools.SQLiteMetaDataHelper(Me._Connection, TX, True)

                'send invalid type of sentanchor.
                _SQLiteMetaDataHelper.SetAnchorValue("TestTableDate", Tools.AnchorType.SentAnchor, "TEXT ANCHOR")
            End Using

        Catch e As ArgumentException
            ' OK
        End Try
    End Sub

    <TestMethod()> _
    Public Sub TestGetClientGUID()
        Try

            Using TX As System.Data.SQLite.SQLiteTransaction = Me._Connection.BeginTransaction
                _SQLiteMetaDataHelper = New Tools.SQLiteMetaDataHelper(Me._Connection, TX, True)
                Dim ClientGuid As System.Guid = Me._SQLiteMetaDataHelper.GetClientGuid

                Assert.AreEqual(ClientGuid, Me._SQLiteMetaDataHelper.GetClientGuid)
            End Using
        Catch e As ArgumentException
            ' OK
        End Try

    End Sub


    <TestCleanup()> _
    Public Sub TearDown()
        Me._Connection.Close()
        Me._Connection.Dispose()
        Me._Connection = Nothing
    End Sub

End Class
