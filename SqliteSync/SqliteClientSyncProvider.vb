Imports Microsoft.Synchronization
Imports Microsoft.Synchronization.Data
Imports System.IO
Imports Tools

Public Class BinaryException
    Inherits Exception

    Public Array As Byte()

    Public Sub New(ByVal arr As Byte())
        Array = arr
    End Sub
End Class


Public Class SqliteClientSyncProvider
    Inherits ClientSyncProvider
    Private _ClientId As Guid = Guid.Empty
    Private _Connection As SQLite.SQLiteConnection
    Private _Transaction As SQLite.SQLiteTransaction

    'Obtain GUID from local database. This is for bidirectional syncronization.


    Private _SQLiteMetaDataHelper As SQLiteMetaDataHelper

    Private _ConflictResolver As SyncConflictResolver

    Public Event ChangesApplied As EventHandler(Of ChangesAppliedEventArgs)
    'Public Event OnSelectingChanges(ByVal args As Microsoft.Synchronization.Data.SelectingChangesEventArgs)
    'Public Event OnChangesSelected(ByVal args As Microsoft.Synchronization.Data.ChangesSelectedEventArgs)
    'Public Event OnCreatingSchema(ByVal args As Microsoft.Synchronization.Data.CreatingSchemaEventArgs)
    'Public Event OnSchemaCreated(ByVal args As Microsoft.Synchronization.Data.SchemaCreatedEventArgs)

    Public Event SyncProgress As EventHandler(Of SyncProgressEventArgs)
    Public Event ApplyChangeFailed As EventHandler(Of ApplyChangeFailedEventArgs)
    Public Event ApplyingChanges As EventHandler(Of ApplyingChangesEventArgs)
    Public Event ChangesSelected As EventHandler(Of ChangesSelectedEventArgs)
    Public Event CreatingSchema As EventHandler(Of CreatingSchemaEventArgs)
    Public Event SchemaCreated As EventHandler(Of SchemaCreatedEventArgs)
    Public Event SelectingChanges As EventHandler(Of SelectingChangesEventArgs)


    'Private _GroupMetadata As SyncGroupMetadata
    'Private _Dataset As DataSet
    'Private _TableMetadata As SyncTableMetadata
    'Private _TableProgress As SyncTableProgress
    'Private _GroupProgress As SyncGroupProgress
    'Private _SyncSession As SyncSession
    'Private _SyncContext As SyncContext

    Sub New(ByVal ConnectionString As String)
        Me.New(New SQLite.SQLiteConnection(ConnectionString))
    End Sub

    Sub New(ByVal Connection As SQLite.SQLiteConnection)
        _ConflictResolver = New SyncConflictResolver
        Me._Connection = Connection
        If Not Me._Connection.State = ConnectionState.Open Then
            _Connection.Open()
            SyncTracer.Verbose("Connecting to sqlite database: {0}", Connection.ConnectionString)
        End If
    End Sub

    ''' <summary>
    ''' Apply changes to local database.
    ''' </summary>
    ''' <param name="groupMetadata"></param>
    ''' <param name="dataSet"></param>
    ''' <param name="syncSession"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Overrides Function ApplyChanges(ByVal groupMetadata As SyncGroupMetadata, ByVal dataSet As System.Data.DataSet, ByVal syncSession As SyncSession) As SyncContext
        Dim SyncContext As New SyncContext
        Dim GroupProgress As New SyncGroupProgress(groupMetadata, dataSet)
        SyncContext.GroupProgress = GroupProgress
        Dim TableProgress As SyncTableProgress
        Dim TableMetadata As SyncTableMetadata
        ''groupMetadata.TablesMetadata(0).LastSentAnchor 

        SyncTracer.Info("----- Client Applying Changes from Server for Group ""{0}"" -----", groupMetadata.GroupName)

        RaiseEvent ApplyingChanges(Me, New ApplyingChangesEventArgs(groupMetadata, dataSet, syncSession, SyncContext, Me._Connection, Me._Transaction))

        For i As Integer = 0 To groupMetadata.TablesMetadata.Count - 1
            If groupMetadata.TablesMetadata(i).SyncDirection = SyncDirection.UploadOnly Then Continue For
            TableMetadata = groupMetadata.TablesMetadata(i)
            If Not dataSet.Tables.Contains(TableMetadata.TableName) Then Continue For

            Dim Table As DataTable = dataSet.Tables(TableMetadata.TableName)
            For iTableProgress As Integer = 0 To GroupProgress.TablesProgress.Count - 1
                If GroupProgress.TablesProgress(i).TableName = Table.TableName Then
                    TableProgress = GroupProgress.TablesProgress(i)
                    Exit For
                End If
            Next

            TableProgress.DataTable = Table





            TableProgress.Updates = GetRowsChanges(Table, DataRowState.Modified)
            TableProgress.Inserts = GetRowsChanges(Table, DataRowState.Added)
            TableProgress.Deletes = GetRowsChanges(Table, DataRowState.Deleted)

            Using SqliteCommandGen As New SQLiteCommandGenerator(Table.TableName, _
                                                                Me._Connection, _
                                                                Me._Transaction)


                If TableMetadata.SyncDirection = SyncDirection.Snapshot Then
                    'TODO: what about vacuum (problem with transactions?)...
                    Using cmdDeleteALL As New SQLite.SQLiteCommand(String.Format("DELETE FROM {0};", TableMetadata.TableName), Me._Connection, Me._Transaction)
                        cmdDeleteALL.ExecuteNonQuery()
                    End Using
                End If

                If Table.GetChanges(DataRowState.Added) IsNot Nothing AndAlso Table.GetChanges(DataRowState.Added).Rows.Count <> 0 Then
                    SyncTracer.Info("")
                    SyncTracer.Verbose(1, "----- Applying Inserts for Table {0} -----", TableMetadata.TableName)
                    ApplyInserts(TableMetadata, _
                                TableProgress, _
                                groupMetadata, _
                                GroupProgress, _
                                Table.GetChanges(DataRowState.Added), _
                                SqliteCommandGen, _
                                SyncContext, _
                                syncSession)
                    SyncTracer.Info("")
                    SyncTracer.Verbose(1, "----- End Applying Inserts for Table {0} -----", TableMetadata.TableName)
                End If

                If Table.GetChanges(DataRowState.Modified) IsNot Nothing AndAlso Table.GetChanges(DataRowState.Modified).Rows.Count <> 0 Then
                    SyncTracer.Info("")
                    SyncTracer.Verbose(1, "----- Applying Updates for Table {0} -----", TableMetadata.TableName)
                    ApplyUpdates(TableMetadata, _
                                TableProgress, _
                                groupMetadata, _
                                GroupProgress, _
                                Table.GetChanges(DataRowState.Modified), _
                                SqliteCommandGen, _
                                SyncContext, _
                                syncSession)
                    SyncTracer.Info("")
                    SyncTracer.Verbose(1, "----- End Applying Updates for Table {0} -----", TableMetadata.TableName)
                End If

                If Table.GetChanges(DataRowState.Deleted) IsNot Nothing AndAlso Table.GetChanges(DataRowState.Deleted).Rows.Count <> 0 Then
                    SyncTracer.Info("")
                    SyncTracer.Verbose(1, "----- Applying Deletes for Table {0} -----", TableMetadata.TableName)
                    ApplyDeletes(TableMetadata, _
                                TableProgress, _
                                groupMetadata, _
                                GroupProgress, _
                                Table.GetChanges(DataRowState.Deleted), _
                                SqliteCommandGen, _
                                SyncContext, _
                                syncSession)
                    SyncTracer.Info("")
                    SyncTracer.Verbose(1, "----- End Applying Deletes for Table {0} -----", TableMetadata.TableName)
                End If
            End Using

        Next

        For Each Table As SyncTableMetadata In groupMetadata.TablesMetadata
            SetTableReceivedAnchor(Table.TableName, groupMetadata.NewAnchor())
        Next

        SyncTracer.Info("----- End Client Applying Changes from Server for Group ""{0}"" -----", groupMetadata.GroupName)
        RaiseEvent ChangesApplied(Me, New Microsoft.Synchronization.Data.ChangesAppliedEventArgs(groupMetadata, syncSession, SyncContext, Me._Connection, _Transaction))
        Return SyncContext
    End Function

    Private Sub SetParameters(ByRef Command As SQLite.SQLiteCommand, ByRef Row As DataRow)
        For Each Param As SQLite.SQLiteParameter In Command.Parameters
            If Param.SourceColumn IsNot Nothing Then
                If Row.RowState = DataRowState.Deleted Then
                    Param.Value = Row.Item(Param.SourceColumn, DataRowVersion.Original)
                Else
                    Param.Value = Row.Item(Param.SourceColumn)
                End If
            End If
        Next
    End Sub

    Private Sub ApplyInserts( _
                            ByVal TableMetadata As SyncTableMetadata, _
                            ByRef TableProgress As SyncTableProgress, _
                            ByVal GroupMetadata As SyncGroupMetadata, _
                            ByRef GroupProgress As SyncGroupProgress, _
                            ByVal DataTable As DataTable, _
                            ByVal SqliteCommandGen As SQLiteCommandGenerator, _
                            ByVal SyncContext As SyncContext, _
                            ByVal SyncSession As SyncSession)


        For Each Row As DataRow In DataTable.Rows
            'Verify if exists row with the same id in the client database.

RetryClientInsertServerInsert:

            Using SelectCountCommand As SQLite.SQLiteCommand = SqliteCommandGen.SelectCountCommand
                SetParameters(SelectCountCommand, Row)
                If CType(SelectCountCommand.ExecuteScalar(), Integer) > 0 Then

                    Dim Conflict As SyncConflict

                    Conflict = New SyncConflict(ConflictType.ClientInsertServerInsert, SyncStage.ApplyingInserts)
                    Conflict.ClientChange = New DataTable

                    Using SelectCommand As SQLite.SQLiteCommand = SqliteCommandGen.SelectCommand
                        SetParameters(SelectCommand, Row)
                        Using Da As New SQLite.SQLiteDataAdapter(SelectCommand)
                            Da.Fill(Conflict.ClientChange)
                        End Using
                    End Using

                    Conflict.ServerChange = DataTable.Clone
                    With Conflict.ServerChange
                        .Rows.Clear()
                        .ImportRow(Row)
                    End With

                    Dim ApplyAction As ApplyAction = _
                        GetActionFromConflict(TableMetadata, Conflict, SyncContext, SyncSession)
                    Select Case ApplyAction
                        Case Data.ApplyAction.Continue
                            TableProgress.ChangesFailed += 1
                            GoTo NextRow
                        Case Data.ApplyAction.RetryApplyingRow
                            For i As Integer = 0 To Conflict.ServerChange.Rows(0).ItemArray.Length - 1
                                Row.Item(i) = Conflict.ServerChange.Rows(0).ItemArray(i)
                            Next
                            GoTo RetryClientInsertServerInsert
                        Case Data.ApplyAction.RetryWithForceWrite
                            Using UpdateCommand As SQLite.SQLiteCommand = SqliteCommandGen.UpdateCommand
                                SetParameters(UpdateCommand, Row)
                                If UpdateCommand.Parameters.Contains("@__sysChangeTxBsn") Then
                                    UpdateCommand.Parameters("@__sysChangeTxBsn").Value = -1 'Prevent the execution of update trigger.
                                End If
                                UpdateCommand.ExecuteNonQuery()
                            End Using
                            TableProgress.ChangesApplied += 1
                            GoTo NextRow 'continue for
                            'Case Data.ApplyAction.RetryNextSync
                            '    TableProgress.ChangesApplied += 1
                            '    Throw New SyncAbortedException("Sync error due a ClientInsertServerInsert conflict.")
                    End Select
                End If
            End Using
            '''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
            'Apply insert to database.
            Using InsertCommand As SQLite.SQLiteCommand = SqliteCommandGen.InsertCommand
                SetParameters(InsertCommand, Row)
                InsertCommand.ExecuteNonQuery()
            End Using
            '''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
            TableProgress.ChangesApplied += 1


NextRow:
            Row.AcceptChanges()
            RaiseEvent SyncProgress(Me, New SyncProgressEventArgs(TableMetadata, TableProgress, GroupMetadata, GroupProgress, SyncStage.ApplyingInserts))
        Next
    End Sub

    Private Sub ApplyUpdates( _
                            ByVal TableMetadata As SyncTableMetadata, _
                            ByRef TableProgress As SyncTableProgress, _
                            ByVal GroupMetadata As SyncGroupMetadata, _
                            ByVal GroupProgress As SyncGroupProgress, _
                            ByVal DataTable As DataTable, _
                            ByVal SqliteCommandGen As SQLiteCommandGenerator, _
                            ByVal SyncContext As SyncContext, _
                            ByVal SyncSession As SyncSession)
        'ConflictType.ClientDeleteServerUpdate
        'ConflictType.ClientUpdateServerUpdate()

        For Each Row As DataRow In DataTable.Rows
RetryClientDeleteServerUpdate:
            'Verify is exists ClienteDeleteServerUpdate conflict.
            Using SelectCountCommand As SQLite.SQLiteCommand = SqliteCommandGen.SelectCountCommand
                SetParameters(SelectCountCommand, Row)
                If CType(SelectCountCommand.ExecuteScalar(), Integer) = 0 Then
                    Dim Conflict As SyncConflict
                    Conflict = New SyncConflict(ConflictType.ClientDeleteServerUpdate, SyncStage.ApplyingUpdates)
                    Conflict.ClientChange = Nothing
                    Conflict.ServerChange = DataTable.Clone

                    With Conflict.ServerChange
                        .Rows.Clear()
                        .ImportRow(Row)
                    End With

                    Dim ApplyAction As ApplyAction = _
                        GetActionFromConflict(TableMetadata, Conflict, SyncContext, SyncSession)
                    Select Case ApplyAction
                        Case Data.ApplyAction.Continue
                            TableProgress.ChangesFailed += 1
                            GoTo NextRow
                        Case Data.ApplyAction.RetryApplyingRow
                            For i As Integer = 0 To Conflict.ServerChange.Rows(0).ItemArray.Length - 1
                                Row.Item(i) = Conflict.ServerChange.Rows(0).ItemArray(i)
                            Next
                            GoTo RetryClientDeleteServerUpdate
                        Case Data.ApplyAction.RetryWithForceWrite
                            'Transform the update into insert.
                            Using InsertCommand As SQLite.SQLiteCommand = SqliteCommandGen.InsertCommand
                                SetParameters(InsertCommand, Row)
                                InsertCommand.ExecuteNonQuery()
                            End Using
                            If TableMetadata.SyncDirection = SyncDirection.Bidirectional Then
                                Using DeleteTombStone As New SQLite.SQLiteCommand(String.Format("DELETE FROM {0}_tombstone WHERE {1}", TableMetadata.TableName, SqliteCommandGen.WhereClause), Me._Connection, Me._Transaction)
                                    DeleteTombStone.Parameters.AddRange(SqliteCommandGen.PrimaryKeys.ToArray)
                                    SetParameters(DeleteTombStone, Row)
                                    DeleteTombStone.ExecuteNonQuery()
                                End Using
                            End If
                            TableProgress.ChangesApplied += 1
                            GoTo NextRow 'continue for
                            'Case Data.ApplyAction.RetryNextSync
                            '    TableProgress.ChangesFailed += 1
                            '    Throw New SyncAbortedException("Sync error due a ClientInsertServerInsert conflict.")
                    End Select
                End If
            End Using
            '''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

RetryClientUpdateServerUpdate:
            'Verify is exists ClienteUpdateServerUpdate conflict.
            If Not TableMetadata.SyncDirection = SyncDirection.DownloadOnly Then
                Dim TextSelectCommandDobleUpdate As String = _
                    String.Format("SELECT * FROM {0} WHERE {1} AND __sysChangeTxBsn > @LastSentAnchor", _
                                    TableMetadata.TableName, _
                                    SqliteCommandGen.WhereClause)
                Using SelectCommandDobleUpdate As New SQLite.SQLiteCommand(TextSelectCommandDobleUpdate, Me._Connection, Me._Transaction)
                    Dim ClientUpdateDataTable As New DataTable
                    SelectCommandDobleUpdate.Parameters.AddRange(SqliteCommandGen.PrimaryKeys.ToArray)
                    SelectCommandDobleUpdate.Parameters.Add("@LastSentAnchor", DbType.Int64).Value = AnchorToInt64(TableMetadata.LastSentAnchor)
                    SetParameters(SelectCommandDobleUpdate, Row)

                    Using DA As New SQLite.SQLiteDataAdapter(SelectCommandDobleUpdate)
                        DA.Fill(ClientUpdateDataTable)
                    End Using
                    RemoveMetaData(ClientUpdateDataTable)

                    'Exists update on client and server sider.
                    If ClientUpdateDataTable.Rows.Count > 0 Then
                        Dim Conflict As SyncConflict
                        Conflict = New SyncConflict(ConflictType.ClientUpdateServerUpdate, SyncStage.ApplyingUpdates)

                        Conflict.ClientChange = ClientUpdateDataTable
                        Conflict.ServerChange = DataTable.Clone

                        With Conflict.ServerChange
                            .Rows.Clear()
                            .ImportRow(Row)
                        End With

                        Dim ApplyAction As ApplyAction = _
                            GetActionFromConflict(TableMetadata, Conflict, SyncContext, SyncSession)
                        Select Case ApplyAction
                            Case Data.ApplyAction.Continue
                                TableProgress.ChangesFailed += 1
                                GoTo NextRow
                            Case Data.ApplyAction.RetryApplyingRow
                                If Conflict.ServerChange.Rows.Count = 0 Then
                                    GoTo NextRow
                                Else
                                    For i As Integer = 0 To Conflict.ServerChange.Rows(0).ItemArray.Length - 1
                                        Row.Item(i) = Conflict.ServerChange.Rows(0).ItemArray(i)
                                    Next
                                End If
                                'Continue with the update 
                            Case Data.ApplyAction.RetryWithForceWrite
                                'Do nothing, continue normally with the update.
                                'Case Data.ApplyAction.RetryNextSync
                                '    TableProgress.ChangesFailed += 1
                                '    Throw New SyncAbortedException("Sync error due a ClientInsertServerInsert conflict.")
                        End Select
                    End If
                End Using
            End If
            '''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
            'Apply update to database.
            Using UpdateCommand As SQLite.SQLiteCommand = SqliteCommandGen.UpdateCommand
                SetParameters(UpdateCommand, Row)
                If UpdateCommand.Parameters.Contains("@__sysChangeTxBsn") Then
                    UpdateCommand.Parameters("@__sysChangeTxBsn").Value = -1 'Prevent the execution of update trigger.
                End If
                UpdateCommand.ExecuteNonQuery()
            End Using
            '''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
            TableProgress.ChangesApplied += 1
NextRow:
            Row.AcceptChanges()
            RaiseEvent SyncProgress(Me, New SyncProgressEventArgs(TableMetadata, TableProgress, GroupMetadata, GroupProgress, SyncStage.ApplyingInserts))
        Next
    End Sub

    Private Sub ApplyDeletes( _
                            ByVal TableMetadata As SyncTableMetadata, _
                            ByRef TableProgress As SyncTableProgress, _
                            ByVal GroupMetadata As SyncGroupMetadata, _
                            ByRef GroupProgress As SyncGroupProgress, _
                            ByVal DataTable As DataTable, _
                            ByVal SqliteCommandGen As SQLiteCommandGenerator, _
                            ByVal SyncContext As SyncContext, _
                            ByVal SyncSession As SyncSession)

        'ConflictType.ClientUpdateServerDelete 
        Dim RowArray As New ArrayList
        Dim DeletedRowCount As Integer = 0
        RowArray.AddRange(DataTable.Rows)


        For Each Row As DataRow In RowArray
            If TableMetadata.SyncDirection = SyncDirection.Bidirectional Then
RetryClientUpdateServerDelete:
                'Verify is exists ClienteDeleteServerUpdate conflict.
                Dim TextSelectCommandDobleUpdate As String = _
                    String.Format("SELECT * FROM {0} WHERE {1} AND __sysChangeTxBsn > @LastSentAnchor", _
                                    TableMetadata.TableName, _
                                    SqliteCommandGen.WhereClause)
                Using SelectCommandDobleUpdate As New SQLite.SQLiteCommand(TextSelectCommandDobleUpdate, Me._Connection, Me._Transaction)
                    Dim ClientUpdateDataTable As New DataTable
                    SelectCommandDobleUpdate.Parameters.AddRange(SqliteCommandGen.PrimaryKeys.ToArray)
                    SelectCommandDobleUpdate.Parameters.Add("@LastSentAnchor", DbType.Int64).Value = AnchorToInt64(TableMetadata.LastSentAnchor)
                    SetParameters(SelectCommandDobleUpdate, Row)

                    Using DA As New SQLite.SQLiteDataAdapter(SelectCommandDobleUpdate)
                        DA.Fill(ClientUpdateDataTable)
                    End Using
                    RemoveMetaData(ClientUpdateDataTable)

                    'Exists update on client and server sider.
                    If ClientUpdateDataTable.Rows.Count > 0 Then
                        Dim Conflict As SyncConflict
                        Conflict = New SyncConflict(ConflictType.ClientUpdateServerDelete, SyncStage.ApplyingDeletes)
                        Conflict.ClientChange = ClientUpdateDataTable
                        Conflict.ServerChange = DataTable.Clone

                        With Conflict.ServerChange
                            .Rows.Clear()
                            .ImportRow(Row)
                        End With

                        Dim ApplyAction As ApplyAction = _
                            GetActionFromConflict(TableMetadata, Conflict, SyncContext, SyncSession)
                        Select Case ApplyAction
                            Case Data.ApplyAction.Continue
                                TableProgress.ChangesFailed += 1
                                GoTo NextRow
                            Case Data.ApplyAction.RetryApplyingRow
                                For i As Integer = 0 To Conflict.ServerChange.Rows(0).ItemArray.Length - 1
                                    Row.Item(i) = Conflict.ServerChange.Rows(0).ItemArray(i)
                                Next
                                GoTo RetryClientUpdateServerDelete
                            Case Data.ApplyAction.RetryWithForceWrite
                                'Do nothing, continue with normally deletion.
                        End Select
                    End If
                End Using
            End If
            '''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
            'Apply update to database.
            Using DeleteCommand As SQLite.SQLiteCommand = SqliteCommandGen.DeleteCommand
                SetParameters(DeleteCommand, Row)
                DeleteCommand.ExecuteNonQuery()
            End Using
            '''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
            TableProgress.ChangesApplied += 1
NextRow:
            Row.AcceptChanges()
            RaiseEvent SyncProgress(Me, New SyncProgressEventArgs(TableMetadata, TableProgress, GroupMetadata, GroupProgress, SyncStage.ApplyingDeletes))
        Next

        SyncTracer.Info(2, "Deletes Applied: {0}", TableProgress.ChangesApplied)
        If TableProgress.ChangesFailed > 0 Then
            SyncTracer.Info(2, "Deletes Failed: {0}", TableProgress.ChangesFailed)
        End If
    End Sub

    ''' <summary>
    ''' Find ApplyAction to resolve the conflict.
    ''' Based on: http://msdn.microsoft.com/en-us/library/bb725997.aspx
    ''' </summary>
    ''' <param name="tableMetadata"></param>
    ''' <param name="syncConflict"></param>
    ''' <param name="syncSession"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Private Function GetActionFromConflict( _
                ByVal tableMetadata As SyncTableMetadata, _
                ByVal syncConflict As SyncConflict, _
                ByVal syncContext As SyncContext, _
                ByVal syncSession As SyncSession) As ApplyAction

        Dim Action As ApplyAction = ApplyAction.Continue
        Dim ResolverAction As ResolveAction = ResolveAction.ClientWins

        Select Case syncConflict.ConflictType
            Case ConflictType.ClientInsertServerInsert
                ResolverAction = Me._ConflictResolver.ClientInsertServerInsertAction
            Case ConflictType.ClientUpdateServerUpdate
                ResolverAction = Me._ConflictResolver.ClientUpdateServerUpdateAction
            Case ConflictType.ClientUpdateServerDelete
                ResolverAction = Me._ConflictResolver.ClientUpdateServerDeleteAction
            Case ConflictType.ClientDeleteServerUpdate
                ResolverAction = Me._ConflictResolver.ClientDeleteServerUpdateAction
            Case Else
                If (Me._ConflictResolver.StoreErrorAction = ResolveAction.FireEvent) Then
                    ResolverAction = ResolveAction.FireEvent
                Else
                    ResolverAction = ResolveAction.ClientWins
                End If
        End Select

        'ClientWins: equivalent to setting an ApplyAction of Continue.
        'ServerWins: equivalent to setting an ApplyAction of RetryWithForceWrite.
        'FireEvent: fire the ApplyChangeFailed event, the default, and then handle the event.


        Select Case ResolverAction
            Case ResolveAction.FireEvent            'This is the default action for all conflicts in the resolver.

                Dim ApplyChangeFailedArgs As New ApplyChangeFailedEventArgs( _
                    tableMetadata, _
                    syncConflict, _
                    New SQLite.SQLiteException, _
                    syncSession, _
                    syncContext, _
                    Me._Connection, _
                    Me._Transaction)

                RaiseEvent ApplyChangeFailed(Me, ApplyChangeFailedArgs)
                Return ApplyChangeFailedArgs.Action
            Case ResolveAction.ServerWins           '
                Return ApplyAction.RetryWithForceWrite
            Case ResolveAction.ClientWins
                Return ApplyAction.Continue
        End Select

    End Function


    Private Function GetRowsChanges(ByVal Table As DataTable, ByVal RowState As DataRowState) As Integer
        Dim NewDataTable As DataTable = Table.GetChanges(RowState)
        If NewDataTable IsNot Nothing Then
            Return NewDataTable.Rows.Count
        Else
            Return Nothing
        End If
    End Function

    'Private Sub RowUpdated(ByVal sender As Object, ByVal e As System.Data.Common.RowUpdatedEventArgs)
    '    Dim SyncProgressStage As Microsoft.Synchronization.SyncStage

    '    Select Case e.StatementType
    '        Case StatementType.Delete
    '            SyncProgressStage = SyncStage.ApplyingDeletes
    '        Case StatementType.Update
    '            SyncProgressStage = SyncStage.ApplyingUpdates
    '        Case StatementType.Insert
    '            SyncProgressStage = SyncStage.ApplyingInserts
    '    End Select

    '    If e.Status = UpdateStatus.ErrorsOccurred Then
    '        Me._TableProgress.ChangesFailed += 1
    '    Else
    '        Me._TableProgress.ChangesApplied += 1
    '    End If

    '    RaiseEvent SyncProgress(Me, New SyncProgressEventArgs(Me._TableMetadata, Me._TableProgress, Me._GroupMetadata, Me._GroupProgress, SyncProgressStage))
    'End Sub

    Public Overrides Sub BeginTransaction(ByVal syncSession As SyncSession)
        _Transaction = _Connection.BeginTransaction(IsolationLevel.ReadCommitted)
        _SQLiteMetaDataHelper = New SQLiteMetaDataHelper(_Connection, _Transaction, True)
        SyncTracer.Verbose("**** Client Provider Begin Transaction ****")
    End Sub

    Public Overrides Sub EndTransaction(ByVal commit As Boolean, ByVal syncSession As SyncSession)
        If commit Then
            _Transaction.Commit()
            SyncTracer.Verbose("**** Client Provider Commit Transaction ****")
        Else
            _Transaction.Rollback()
            SyncTracer.Verbose("**** Client Provider Rollback Transaction ****")
        End If
    End Sub

    Public Overrides Property ClientId() As System.Guid
        Get
            If Me._ClientId = Guid.Empty Then
                Me._ClientId = Me._SQLiteMetaDataHelper.GetClientGuid()
            End If
            'Load guid from db.
            Return Me._clientid
        End Get
        Set(ByVal value As System.Guid)
            Throw New InvalidOperationException()
        End Set
    End Property

    ''' <summary>
    ''' Get locally made inserts between LastSent and NewLastSent.
    ''' </summary>
    ''' <param name="LastSent"></param>
    ''' <param name="NewLastSent"></param>
    ''' <param name="DataTable"></param>
    ''' <param name="TableProgress"></param>
    ''' <remarks></remarks>
    Private Sub EnumerateInserts(ByVal LastSent As Int64, ByVal NewLastSent As Int64, ByVal DataTable As DataTable, ByRef TableProgress As SyncTableProgress, ByVal SyncDir As SyncDirection)
        Dim GetInsertsCommand As String = "SELECT * " & _
                                        "FROM " & DataTable.TableName & " " & _
                                        "WHERE __sysInsertTxBsn > @LastSent " & _
                                        "AND __sysInsertTxBsn <= @NewLastSent " & _
                                        IIf(SyncDir = SyncDirection.Bidirectional, "AND COALESCE(__sysReceived, 0) = 0", "")

        SyncTracer.Info("")
        SyncTracer.Info(1, "----- Enumerating Inserts for Table {0} -----", DataTable.TableName)

        Using cmdGetInserts As New SQLite.SQLiteCommand( _
                    GetInsertsCommand, Me._Connection, Me._Transaction)

            cmdGetInserts.Parameters.Add("@LastSent", DbType.Int64).Value = LastSent
            cmdGetInserts.Parameters.Add("@NewLastSent", DbType.Int64).Value = NewLastSent
            Using da As New SQLite.SQLiteDataAdapter(cmdGetInserts)
                TableProgress.Inserts = da.Fill(DataTable)
            End Using
            For Each Row As DataRow In DataTable.Rows
                Row.SetAdded()
            Next
        End Using
        SyncTracer.Info(2, "Changes Enumerated: {0}", TableProgress.Inserts)
        SyncTracer.Info(1, "--- End Enumerating Inserts for Table {0} ---", DataTable.TableName)
    End Sub

    ''' <summary>
    ''' Get updates made between LastSent and NewLastSent.
    ''' </summary>
    ''' <param name="LastSent"></param>
    ''' <param name="NewLastSent"></param>
    ''' <param name="DataTable"></param>
    ''' <param name="TableProgress"></param>
    ''' <remarks></remarks>
    Private Sub EnumerateUpdates(ByVal LastSent As Int64, ByVal NewLastSent As Int64, ByVal DataTable As DataTable, ByRef TableProgress As SyncTableProgress, ByVal SyncDir As SyncDirection)
        Dim SbUpdateCommand As New System.Text.StringBuilder

        SbUpdateCommand.AppendFormat("SELECT * " & _
                                    "FROM {0} " & _
                                    "WHERE {1} > @LastSent " & _
                                    "AND {1} <= @NewLastSent", _
                                    DataTable.TableName, _
                                    "__sysChangeTxBsn")
        SbUpdateCommand.AppendLine()

        If SyncDir = SyncDirection.Bidirectional Then
            SbUpdateCommand.AppendFormat(" AND ({0} <= @LastSent OR COALESCE(__SysReceived, 0) = 1)", "__sysInsertTxBsn")
        Else  'If SyncDir = SyncDirection.UploadOnly Then
            SbUpdateCommand.AppendFormat(" AND {0} <= @LastSent ", "__sysInsertTxBsn")
        End If


        SyncTracer.Info("")
        SyncTracer.Info(1, "----- Enumerating Updates for Table {0} -----", DataTable.TableName)


        Using cmdGetUpdates As New SQLite.SQLiteCommand(SbUpdateCommand.ToString, Me._Connection, Me._Transaction)
            cmdGetUpdates.Parameters.Add("@LastSent", DbType.Int64).Value = LastSent
            cmdGetUpdates.Parameters.Add("@NewLastSent", DbType.Int64).Value = NewLastSent
            Using da As New SQLite.SQLiteDataAdapter(cmdGetUpdates)
                TableProgress.Updates = da.Fill(DataTable)
            End Using
            For Each Row As DataRow In DataTable.Rows
                If Row.RowState <> DataRowState.Added Then
                    Row.SetModified()
                End If
            Next
        End Using
        SyncTracer.Info(2, "Changes Enumerated: {0}", TableProgress.Updates)
        SyncTracer.Info(1, "--- End Enumerating Updates for Table {0} ---", DataTable.TableName)
    End Sub


    ''' <summary>
    ''' Get deletes made between LastSent and NewLastSent.
    ''' </summary>
    ''' <param name="LastSent"></param>
    ''' <param name="NewLastSent"></param>
    ''' <param name="DataTable"></param>
    ''' <param name="TableProgress"></param>
    ''' <remarks></remarks>
    Private Sub EnumerateDeletes(ByVal LastSent As Int64, ByVal NewLastSent As Int64, ByVal DataTable As DataTable, ByRef TableProgress As SyncTableProgress, ByVal SyncDir As SyncDirection)
        '__SysReceived=0->client added, client deleted.
        '__SysReceived=1->server added, client deleted.

        Dim SbDeleteCommand As New System.Text.StringBuilder
        Dim PKArrayList As ArrayList

        SyncTracer.Info("")
        SyncTracer.Info(1, "----- Enumerating Deletes for Table {0} -----", DataTable.TableName)

        SbDeleteCommand.AppendFormat("SELECT * " & _
                                    "FROM {0}_tombstone " & _
                                    "WHERE {1} > @LastSent " & _
                                    "AND {1} <= @NewLastSent", _
                                    DataTable.TableName, _
                                    "__sysChangeTxBsn")
        SbDeleteCommand.AppendLine()

        If SyncDir = SyncDirection.Bidirectional Then
            SbDeleteCommand.AppendFormat(" AND (COALESCE({0},0) <= @LastSent OR COALESCE(__SysReceived, 0) = 1)", "__sysInsertTxBsn")
        Else  'If SyncDir = SyncDirection.UploadOnly Then
            SbDeleteCommand.AppendFormat(" AND COALESCE({0},0) <= @LastSent ", "__sysInsertTxBsn")
        End If


        Using cmdGetDeletes As New SQLite.SQLiteCommand(SbDeleteCommand.ToString, Me._Connection, Me._Transaction)
            cmdGetDeletes.Parameters.Add("@LastSent", DbType.Int64).Value = LastSent
            cmdGetDeletes.Parameters.Add("@NewLastSent", DbType.Int64).Value = NewLastSent
            Using da As New SQLite.SQLiteDataAdapter(cmdGetDeletes)
                TableProgress.Deletes = da.Fill(DataTable)
            End Using

            If SyncTracer.IsVerboseEnabled Then
                Using CmdGenerator As New SQLiteCommandGenerator(DataTable.TableName, Me._Connection, Me._Transaction)
                    PKArrayList = CmdGenerator.PrimaryKeys
                End Using
            End If

            For Each Row As DataRow In DataTable.Rows
                If Row.RowState = DataRowState.Unchanged Then

                    If SyncTracer.IsVerboseEnabled Then
                        Dim strKeys As String = ""
                        For Each PrimaryKey As String In PKArrayList
                            strKeys += Row.Item(PrimaryKey) + " | "
                        Next
                        SyncTracer.Verbose(2, "Delete for row with PK: {0}", strKeys)
                    End If
                    Row.Delete()
                End If
            Next
            SyncTracer.Info(2, "Changes Enumerated: {0}", TableProgress.Deletes)
            SyncTracer.Info(1, "--- End Enumerating Deletes for Table {0} ---", DataTable.TableName)
        End Using
    End Sub

    Private Shared Sub RemoveMetaData(ByRef Dataset As DataSet)
        For Each Table As DataTable In Dataset.Tables
            RemoveMetaData(Table)
        Next
    End Sub

    Private Shared Sub RemoveMetaData(ByRef Table As DataTable)
        If Table.Columns.Contains("__sysChangeTxBsn") Then
            Table.Columns.Remove("__sysChangeTxBsn")
        End If
        If Table.Columns.Contains("__sysInsertTxBsn") Then
            Table.Columns.Remove("__sysInsertTxBsn")
        End If
        If Table.Columns.Contains("__sysReceived") Then
            Table.Columns.Remove("__sysReceived")
        End If
    End Sub

    Public Overrides Function GetChanges(ByVal groupMetadata As SyncGroupMetadata, ByVal syncSession As SyncSession) As SyncContext
        'Dim o As New Microsoft.Synchronization.Data.
        'Throw New NotImplementedException("This method is not implemented at the moment.")
        'Get inserts... where insertdate > lastsent 
        'Get Updates, but not recently inserted... where insertdate < lastsent and updatedate > lastsent
        'Get deletes, but not recently inserted ... where insertdate < lastsent and updatedate > lastsent
        Dim Context As New SyncContext
        Dim LastSent As Int64
        Dim NewLastSent As Int64 = Me._SQLiteMetaDataHelper.getLastAnchor()

        Context.DataSet = New DataSet

        Dim GroupProgress As New SyncGroupProgress(groupMetadata, Context.DataSet)

        Context.GroupProgress = GroupProgress

        BeginTransaction(syncSession)

        SyncTracer.Info("----- Client Enumerating Changes to Server for Group ""{0}"" -----", groupMetadata.GroupName)

        RaiseEvent SelectingChanges(Me, New SelectingChangesEventArgs(groupMetadata, syncSession, Context, Me._Connection, Me._Transaction))
        
        For Each Table As SyncTableMetadata In groupMetadata.TablesMetadata
            'Ignore tables that not requires upload.
            If Not (Table.SyncDirection = SyncDirection.Bidirectional Or Table.SyncDirection = SyncDirection.UploadOnly) Then Continue For

            'Convert lastAnchor to datetime.
            If Table.LastSentAnchor IsNot Nothing AndAlso Table.LastSentAnchor.Anchor IsNot Nothing Then
                LastSent = CType(DeserializeAnchorValue(Table.LastSentAnchor.Anchor), Int64)
            Else
                LastSent = 0
            End If

            'Create datatable anda tableprogress.
            Dim DataTable As DataTable = Context.DataSet.Tables.Add(Table.TableName)
            Dim TableProgress As New SyncTableProgress(Table.TableName)
            TableProgress.DataTable = DataTable

            'enumerate inserts.
            EnumerateInserts(LastSent, NewLastSent, DataTable, TableProgress, Table.SyncDirection)
            RaiseEvent SyncProgress(Me, New SyncProgressEventArgs(Table, TableProgress, groupMetadata, GroupProgress, SyncStage.GettingInserts))

            'enumerate updates.
            EnumerateUpdates(LastSent, NewLastSent, DataTable, TableProgress, Table.SyncDirection)
            RaiseEvent SyncProgress(Me, New SyncProgressEventArgs(Table, TableProgress, groupMetadata, GroupProgress, SyncStage.GettingUpdates))

            'enumerate deletes.
            EnumerateDeletes(LastSent, NewLastSent, DataTable, TableProgress, Table.SyncDirection)
            RaiseEvent SyncProgress(Me, New SyncProgressEventArgs(Table, TableProgress, groupMetadata, GroupProgress, SyncStage.GettingDeletes))
        Next

        ''Clean the dataset. Remove metadata schema 
        RemoveMetaData(Context.DataSet)

        RaiseEvent ChangesSelected(Me, New ChangesSelectedEventArgs(groupMetadata, syncSession, Context, Me._Connection, Me._Transaction))

        SyncTracer.Info("--- End Client Enumerating Changes to Server for Group ""{0}"" ---", groupMetadata.GroupName)

        EndTransaction(True, syncSession)

        Context.NewAnchor = New SyncAnchor(SerializeAnchorValue(NewLastSent))
        Return Context
    End Function

    Public Overrides Sub CreateSchema(ByVal syncTable As SyncTable, ByVal syncSchema As SyncSchema)
        RaiseEvent CreatingSchema(Me, New CreatingSchemaEventArgs(syncTable, syncSchema, Me._Connection, Me._Transaction))
        Dim DeleteAll As Boolean = False
        Dim DropTable As Boolean = False
        Dim TableExists As Boolean = _SQLiteMetaDataHelper.TableExists(syncTable.TableName)

        Select Case syncTable.CreationOption
            Case TableCreationOption.CreateNewTableOrFail
                If TableExists Then
                    Throw New SchemaException(String.Format("Table already exists {0}.", syncTable.TableName))
                End If
            Case TableCreationOption.DropExistingOrCreateNewTable
                DropTable = TableExists
            Case TableCreationOption.TruncateExistingOrCreateNewTable
                If TableExists Then
                    DeleteAll = True
                End If
            Case TableCreationOption.UploadExistingOrCreateNewTable
                Throw New NotImplementedException("UploadExistingOrCreateNewTable is not yet implemented (lag of documentation)." + Environment.NewLine + _
                                                    "This option is useless. If you want to add to sync control a pre-existent db, " + Environment.NewLine + _
                                                    "start from a clean db and them import the existing data to the new one.")

                'Using cmdSetChangeAndInsertToNull as New SQLite.SQLiteCommand("UPDATE [{0}] SET __sysChangeTxBsn = NULL, "
            Case TableCreationOption.UseExistingTableOrFail
                If Not TableExists Then
                    Throw New SchemaException(String.Format("Table does not exists {0}.", syncTable.TableName))
                End If
        End Select

        If DeleteAll Then
            Using cmdDeleteAll As New SQLite.SQLiteCommand(String.Format("DELETE FROM [{0}];", syncTable.TableName), Me._Connection, Me._Transaction)
                cmdDeleteAll.ExecuteNonQuery()
            End Using
            If _SQLiteMetaDataHelper.TableExists(syncTable.TableName & "_tombstone") Then
                Using cmdDeleteAll As New SQLite.SQLiteCommand(String.Format("DELETE FROM [{0}_tombstone];", syncTable.TableName), Me._Connection, Me._Transaction)
                    cmdDeleteAll.ExecuteNonQuery()
                End Using
            End If
        End If
        If DropTable Then
            Using cmdDropTable As New SQLite.SQLiteCommand(String.Format("DROP TABLE [{0}];", syncTable.TableName), Me._Connection, Me._Transaction)
                cmdDropTable.ExecuteNonQuery()
            End Using
        End If

        Using cmdCreateTable As New SQLite.SQLiteCommand(BuildCreateCommand(syncTable, syncSchema), Me._Connection, Me._Transaction)
            SyncTracer.Verbose("Creating table {0} with command: {1}", syncTable.TableName, cmdCreateTable.CommandText)
            cmdCreateTable.ExecuteNonQuery()
        End Using


        RaiseEvent SchemaCreated(Me, New SchemaCreatedEventArgs(syncTable, syncSchema, Me._Connection, Me._Transaction))
    End Sub

    ''' <summary>
    ''' Builds the CREATE TABLE command for the create schema method.
    ''' </summary>
    ''' <param name="syncTable"></param>
    ''' <param name="syncSchema"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Private Function BuildCreateCommand(ByVal syncTable As SyncTable, ByVal syncSchema As SyncSchema) As String
        Dim StringBuilder As New Text.StringBuilder
        Dim Table As SyncSchemaTable = syncSchema.Tables(syncTable.TableName)
        Dim TypeMapping As New SQLiteTypeMapping

        StringBuilder.Append("CREATE TABLE " & Table.TableName).Append(" (").AppendLine()

        'Column definition : name [type] [[CONSTRAINT name] column-constraint]*
        'http://www.sqlite.org/lang_createtable.html
        StringBuilder.ToString()
        For i As Integer = 0 To Table.Columns.Count - 1
            Dim Column As SyncSchemaColumn = Table.Columns(i)
            'Column Name
            StringBuilder.AppendFormat("[{0}] ", Column.ColumnName)

            'Column Type
            If Not Column.AutoIncrement Then
                StringBuilder.AppendFormat(TypeMapping.GetCreateTableType(Column.DataType, Column.ProviderDataType))
            Else
                'AUTOINCREMENT is only allowed on an INTEGER PRIMARY KEY
                StringBuilder.AppendFormat("INTEGER")
            End If
            'Lenght for varchar columns.
            If Column.DataType.Equals(GetType(System.String)) Or Column.DataType.Equals(GetType(System.Char)) Then
                If Column.MaxLength > 0 Then
                    StringBuilder.AppendFormat("({0})", Column.MaxLength)
                End If
            End If

            'Precision and scale.
            If Column.NumericPrecision > 0 OrElse Column.NumericScale > 0 Then
                StringBuilder.AppendFormat("({0},{1})", Column.NumericPrecision, Column.NumericScale)
            End If

            'Is the only PrimaryKey?
            If System.Array.IndexOf(Table.PrimaryKey, Column.ColumnName) >= 0 AndAlso Table.PrimaryKey.Length = 1 Then
                StringBuilder.AppendFormat(" PRIMARY KEY ")
                If Column.AutoIncrement Then
                    StringBuilder.AppendFormat(" AUTOINCREMENT ")
                End If
            End If

            'Allow Null?
            If Not Column.AllowNull Then
                StringBuilder.Append(" NOT NULL ")
            End If

            'Unique?
            If Column.Unique Then
                StringBuilder.Append(" UNIQUE ")
            End If

            'not last column?
            If i < Table.Columns.Count - 1 Then
                StringBuilder.Append(",")
            End If

            StringBuilder.AppendLine()
        Next

        'metadata columns for bidirectional syncronization.
        If syncTable.SyncDirection = SyncDirection.Bidirectional Or syncTable.SyncDirection = SyncDirection.UploadOnly Then
            StringBuilder.Append(", [__sysInsertTxBsn] LONG NULL, [__sysChangeTxBsn] LONG NULL")
            If syncTable.SyncDirection = SyncDirection.Bidirectional Then
                StringBuilder.Append(", [__sysReceived] BOOLEAN DEFAULT 0 NULL")
            End If
        End If

        'Multi-column primary key.
        If Table.PrimaryKey.Length > 1 Then
            StringBuilder.Append(",")
            StringBuilder.AppendFormat("PRIMARY KEY ({0})", String.Join(",", Table.PrimaryKey))
        End If

        'Foreign key constraints
        'NOTE: SQLite doesn't verify that constraints are valid during creation
        If Table.ForeignKeys.Count > 0 Then
            StringBuilder.Append(",")
            StringBuilder.AppendLine()
        End If
        For i As Integer = 0 To Table.ForeignKeys.Count - 1
            Dim ForeignKey As SyncSchemaForeignKey = Table.ForeignKeys(i)
            StringBuilder.Append("FOREIGN KEY (")
            For j As Integer = 0 To ForeignKey.ParentColumns.Count - 1
                Dim ColName As String = ForeignKey.ParentColumns(j)
                StringBuilder.Append(ColName)
                'not last column?
                If j < ForeignKey.ParentColumns.Count - 1 Then
                    StringBuilder.Append(", ")
                End If
            Next
            StringBuilder.Append(") REFERENCES ")
            StringBuilder.Append(ForeignKey.ChildTable)
            StringBuilder.Append(" (")
            For j As Integer = 0 To ForeignKey.ChildColumns.Count - 1
                Dim ColName As String = ForeignKey.ChildColumns(j)
                StringBuilder.Append(ColName)
                'not last column?
                If j < ForeignKey.ChildColumns.Count - 1 Then
                    StringBuilder.Append(", ")
                End If
            Next
            StringBuilder.Append(")")
            StringBuilder.Append(" ON DELETE ")
            Select Case ForeignKey.DeleteRule
                Case SyncSchemaForeignKeyRule.SetDefault
                    StringBuilder.Append("SET DEFAULT")
                Case SyncSchemaForeignKeyRule.SetNull
                    StringBuilder.Append("SET NULL")
                Case SyncSchemaForeignKeyRule.Cascade
                    StringBuilder.Append("CASCADE")
                Case Else
                    StringBuilder.Append("RESTRICT")
            End Select
            StringBuilder.Append(" ON UPDATE ")
            Select Case ForeignKey.UpdateRule
                Case SyncSchemaForeignKeyRule.SetDefault
                    StringBuilder.Append("SET DEFAULT")
                Case SyncSchemaForeignKeyRule.SetNull
                    StringBuilder.Append("SET NULL")
                Case SyncSchemaForeignKeyRule.Cascade
                    StringBuilder.Append("CASCADE")
                Case Else
                    StringBuilder.Append("RESTRICT")
            End Select
            StringBuilder.AppendLine()
        Next
        StringBuilder.Append(");")



        'If bidirectional then add metadata schema.
        If syncTable.SyncDirection = SyncDirection.Bidirectional Or syncTable.SyncDirection = SyncDirection.UploadOnly Then
            'tombstome table

            StringBuilder.AppendLine()
            StringBuilder.Append(StringBuilder.ToString.Replace( _
                                            "CREATE TABLE " & Table.TableName, _
                                            "CREATE TABLE " & Table.TableName & "_tombstone"))


            'Delete trigger
            StringBuilder.AppendLine(TriggerBuilder.GetDeleteTrigger(Table.TableName, syncTable.SyncDirection))

            'Insert trigger
            StringBuilder.AppendLine(TriggerBuilder.GetInsertTrigger(Table.TableName, syncTable.SyncDirection))

            'Update trigger
            StringBuilder.AppendLine(TriggerBuilder.GetUpdateTrigger(Table.TableName))

        End If

        Return StringBuilder.ToString

    End Function

    Public Overrides Sub Dispose()


        'Obtain GUID from local database. This is for bidirectional syncronization.

        Me._ConflictResolver = Nothing
        Me._SQLiteMetaDataHelper = Nothing

        If Me._Connection IsNot Nothing AndAlso Me._Connection.State = ConnectionState.Open Then
            _Connection.Close()
        End If
        If Me._Connection IsNot Nothing Then
            _Connection.Dispose()
        End If
        _Connection = Nothing
    End Sub

#Region "Anchor treatment"

    Public Overrides Function GetTableReceivedAnchor(ByVal tableName As String) As SyncAnchor
        If Not Me._SQLiteMetaDataHelper.TableExists(tableName) Then
            Return Nothing
        Else
            Dim objAnchor As Object = Me._SQLiteMetaDataHelper.GetAnchorValue(tableName, AnchorType.ReceivedAnchor)
            If objAnchor IsNot DBNull.Value Then
                Return New SyncAnchor(SerializeAnchorValue(objAnchor))
            Else
                Return New SyncAnchor()
            End If
        End If
    End Function

    Public Overrides Function GetTableSentAnchor(ByVal tableName As String) As SyncAnchor
        If Not Me._SQLiteMetaDataHelper.TableExists(tableName) Then
            Return Nothing
        Else
            Dim objAnchor As Object = Me._SQLiteMetaDataHelper.GetAnchorValue(tableName, AnchorType.SentAnchor)
            If objAnchor IsNot DBNull.Value Then
                If Not TypeOf objAnchor Is Int64 Then
                    Throw New InvalidCastException
                End If
                Return New SyncAnchor(SerializeAnchorValue(CType(objAnchor, Int64)))
            Else
                'Dim oNewAnchor As New SyncAnchor()
                'oNewAnchor.Anchor = SerializeAnchorValue(DateTime.MinValue)
                Return New SyncAnchor() ' oNewAnchor
            End If
        End If
    End Function

    Public Overrides Sub SetTableReceivedAnchor(ByVal tableName As String, ByVal anchor As SyncAnchor)
        If anchor Is Nothing Then Exit Sub 'Snapshot mode.
        Dim ReceivedAnchor As Object = DeserializeAnchorValue(anchor.Anchor)
        'Sql2000 / 2005 deserialized anchor will be byte()  sql 2008 long.
        Me._SQLiteMetaDataHelper.SetAnchorValue(tableName, AnchorType.ReceivedAnchor, ReceivedAnchor)
    End Sub

    Public Overrides Sub SetTableSentAnchor(ByVal tableName As String, ByVal anchor As SyncAnchor)
        Dim AnchorValue As Int64
        If TypeOf DeserializeAnchorValue(anchor.Anchor) Is Byte() Then
            AnchorValue = 0
        Else
            AnchorValue = DeserializeAnchorValue(anchor.Anchor)
        End If
        'AnchorValue will be datetime.
        Me._SQLiteMetaDataHelper.SetAnchorValue(tableName, AnchorType.SentAnchor, AnchorValue)
    End Sub

    Private Enum AnchorValueType As Byte
        'Null = 0
        Int64 = 1
        ByteArray = 2
    End Enum


    Private Function SerializeAnchorValue(ByVal anchorVal As Object) As Byte()
        If anchorVal Is Nothing Then Return Nothing

#If PocketPC Then
        Dim serializationStream As New MemoryStream()
        Dim BF As New SQLiteSync.Serialization.BinaryFormatter()

        BF.Serialize(serializationStream, anchorVal)

        Dim ret As Byte() = serializationStream.ToArray()

        serializationStream.Dispose()

        Return ret
#Else
        Dim serializationStream As New MemoryStream()
        Dim BF As New System.Runtime.Serialization.Formatters.Binary.BinaryFormatter()

        BF.Serialize(serializationStream, anchorVal)

        Dim ret As Byte() = serializationStream.ToArray()

        serializationStream.Dispose()

        Return ret
#End If
    End Function

    Private Function DeserializeAnchorValue(ByVal anchor As Byte()) As Object
        If anchor Is Nothing Or anchor.Length = 0 Then Return Nothing

#If PocketPC Then
        Dim serializationStream As New MemoryStream(anchor)
        Dim BF As New SQLiteSync.Serialization.BinaryFormatter()
        Dim ret As Object = BF.Deserialize(serializationStream)

        serializationStream.Dispose()

        Return ret
#Else
        Dim serializationStream As New MemoryStream(anchor)
        Dim BF As New System.Runtime.Serialization.Formatters.Binary.BinaryFormatter()
        Dim ret As Object = BF.Deserialize(serializationStream)

        serializationStream.Dispose()

        Return ret
#End If
    End Function

    Private Function AnchorToInt64(ByVal Anchor As SyncAnchor) As Int64
        Dim Result As Int64
        If Anchor IsNot Nothing AndAlso Anchor.Anchor IsNot Nothing Then
            'Result = DateTime.FromBinary(BitConverter.ToInt64(DeserializeAnchorValue(Anchor.Anchor), 0))
            Result = CType(DeserializeAnchorValue(Anchor.Anchor), Int64)
        Else
            Result = 0
        End If
        Return Result
    End Function

    'Private Function DatetimeToAnchor(ByVal Datetime As DateTime) As SyncAnchor
    '    'not need for this moment.
    '    Throw New NotImplementedException
    'End Function
#End Region
End Class
