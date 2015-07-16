Namespace Tools


    ''' <summary>
    ''' Generate the commands for apply the updates.
    ''' SQLiteCommandBuilder cannot be used for this work.
    ''' </summary>
    ''' <remarks></remarks>
    Friend Class SQLiteCommandGenerator
        Implements IDisposable

        Private _PrimaryKeys As New ArrayList  'List(Of SQLite.SQLiteParameter)
        Private _CommonColumns As New ArrayList  'List(Of SQLite.SQLiteParameter)


        Private _InsertCommand As SQLite.SQLiteCommand
        Private _UpdateCommand As SQLite.SQLiteCommand
        Private _DeleteCommand As SQLite.SQLiteCommand
        Private _SelectCommand As SQLite.SQLiteCommand
        Private _SelectCountCommand As SQLite.SQLiteCommand
        Private _SelectTombstoneCommand As SQLite.SQLiteCommand

        Private _SQLiteMetaDataHelper As SQLiteMetaDataHelper
        Private _Connection As SQLite.SQLiteConnection
        Private _Transaction As SQLite.SQLiteTransaction
        Private _TableName As String
        Private _ContainsSysReceived As Boolean = False
        Private _ContainsSysInsert As Boolean = False
        Private _ContainsSysUpdate As Boolean = False
        Private _Where As String = ""

        Sub New(ByVal TableName As String, ByVal Connection As SQLite.SQLiteConnection, ByVal Transaction As SQLite.SQLiteTransaction)

            Dim SelectCommand As New SQLite.SQLiteCommand(String.Format("SELECT * FROM {0}", TableName), Connection, Transaction)
            Me._TableName = TableName
            Me._Connection = Connection
            Me._Transaction = Transaction

            Me._SQLiteMetaDataHelper = New SQLiteMetaDataHelper(Me._Connection, Me._Transaction, True)

            'For each primary key in the table.. add to list of sqliteparameters.
            Using dr As SQLite.SQLiteDataReader = SelectCommand.ExecuteReader(CommandBehavior.KeyInfo)
                Using dt As DataTable = dr.GetSchemaTable
                    For Each Row As DataRow In dt.Rows

                        Select Case CType(Row.Item("ColumnName"), String)
                            'The table has received column. (bidirectional)
                            Case "__sysReceived"
                                Me._ContainsSysReceived = True
                            Case "__sysInsertTxBsn"
                                Me._ContainsSysInsert = True
                            Case "__sysChangeTxBsn"
                                Me._ContainsSysUpdate = True
                            Case Else
                                Dim Param As New SQLite.SQLiteParameter( _
                                        "@" & CType(Row.Item("ColumnName"), String), _
                                        Row.Item("ProviderType"), _
                                        CType(Row.Item("ColumnName"), String), _
                                        DataRowVersion.Current)
                                If CType(Row.Item("IsKey"), Boolean) Then
                                    _PrimaryKeys.Add(Param)
                                Else
                                    _CommonColumns.Add(Param)
                                End If
                        End Select
                    Next
                End Using
            End Using

            BuildWhere()
            BuildInsertCommand()
            BuildUpdateCommand()
            BuildDeleteCommand()
            BuildSelectCommand()

        End Sub

        Private Sub BuildInsertCommand()
            Dim AffectedCols As String = ""
            Dim AffectedColsValues As String = ""

            Dim Command As New SQLite.SQLiteCommand

            Command.Connection = Me._Connection
            Command.Transaction = Me._Transaction

            For Each Param As SQLite.SQLiteParameter In _PrimaryKeys
                AffectedCols += Param.SourceColumn & ","
                AffectedColsValues += Param.ParameterName & ","
                Command.Parameters.Add(Param)
            Next

            For Each Param As SQLite.SQLiteParameter In _CommonColumns
                AffectedCols += Param.SourceColumn & ","
                AffectedColsValues += Param.ParameterName & ","
                Command.Parameters.Add(Param)
            Next


            AffectedCols = AffectedCols.Substring(0, AffectedCols.Length - 1)
            AffectedColsValues = AffectedColsValues.Substring(0, AffectedColsValues.Length - 1)


            If Me._ContainsSysReceived Then
                Command.CommandText = String.Format("INSERT INTO {0}({1},__sysReceived) values({2}, 1);", Me._TableName, AffectedCols, AffectedColsValues)
            Else
                Command.CommandText = String.Format("INSERT INTO {0}({1}) values({2});", Me._TableName, AffectedCols, AffectedColsValues)
            End If

            Me._InsertCommand = Command
        End Sub

        Private Sub BuildUpdateCommand()
            Dim SetClause As String = ""

            If Me._CommonColumns Is Nothing OrElse Me._CommonColumns.Count = 0 Then Exit Sub

            Dim Command As New SQLite.SQLiteCommand

            Command.Connection = Me._Connection
            Command.Transaction = Me._Transaction

            For Each Param As SQLite.SQLiteParameter In Me._PrimaryKeys
                Command.Parameters.Add(Param)
            Next

            For Each Param As SQLite.SQLiteParameter In Me._CommonColumns
                SetClause += Param.SourceColumn & " = " & Param.ParameterName & ","
                Command.Parameters.Add(Param)
            Next

            If Me._ContainsSysUpdate Then
                SetClause += "__sysChangeTxBsn = @__sysChangeTxBsn,"
                Command.Parameters.Add("@__sysChangeTxBsn", DbType.Int64)
            End If

            SetClause = SetClause.Substring(0, SetClause.Length - 1)

            Command.CommandText = String.Format("UPDATE {0} SET {1} WHERE {2};", Me._TableName, SetClause, Me._Where)

            Me._UpdateCommand = Command
        End Sub

        Private Sub BuildDeleteCommand()
            Dim Command As New SQLite.SQLiteCommand

            Command.Connection = Me._Connection
            Command.Transaction = Me._Transaction

            For Each Param As SQLite.SQLiteParameter In Me._PrimaryKeys
                Command.Parameters.Add(Param)
            Next

            If Not Me._SQLiteMetaDataHelper.TableExists(Me._TableName & "_tombstone") Then
                Command.CommandText = String.Format("DELETE FROM {0} WHERE {1};", Me._TableName, Me._Where)
            Else
                'delete from tombstone too.
                Command.CommandText = String.Format("DELETE FROM {0} WHERE {1} AND NOT EXISTS(SELECT * FROM {0}_tombstone WHERE {1}); " & _
                                                    "DELETE FROM {0}_tombstone WHERE {1};" _
                                                    , Me._TableName, Me._Where)
            End If

            Me._DeleteCommand = Command
        End Sub

        Private Sub BuildSelectCommand()
            Dim Cols As String = ""
            Dim Command As New SQLite.SQLiteCommand
            Dim CommandCount As SQLite.SQLiteCommand

            Command.Connection = Me._Connection
            Command.Transaction = Me._Transaction

            For Each Param As SQLite.SQLiteParameter In _PrimaryKeys
                Cols += Param.SourceColumn & ","
                Command.Parameters.Add(Param)
            Next

            For Each Param As SQLite.SQLiteParameter In _CommonColumns
                Cols += Param.SourceColumn & ","
            Next

            Cols = Cols.Substring(0, Cols.Length - 1)

            Command.CommandText = String.Format("SELECT {0} FROM {1} WHERE {2}", Cols, Me._TableName, Me._Where)

            CommandCount = Command.Clone
            CommandCount.CommandText = String.Format("SELECT count(*) FROM {0} WHERE {1}", Me._TableName, Me._Where)

            _SelectTombstoneCommand = Command.Clone
            _SelectTombstoneCommand.CommandText = String.Format("SELECT * FROM {0}_tombstone WHERE {1}", Me._TableName, Me._Where)

            Me._SelectCountCommand = CommandCount
            Me._SelectCommand = Command

        End Sub

        Private Sub BuildWhere()
            For Each Param As SQLite.SQLiteParameter In _PrimaryKeys
                _Where += Param.SourceColumn & " = " & Param.ParameterName & " AND "
            Next
            'remove last and.
            _Where = _Where.Substring(0, _Where.Length - 5)
        End Sub

        Public ReadOnly Property UpdateCommand() As SQLite.SQLiteCommand
            Get
                Return _UpdateCommand.Clone
            End Get
        End Property

        Public ReadOnly Property DeleteCommand() As SQLite.SQLiteCommand
            Get
                Return _DeleteCommand.Clone
            End Get
        End Property

        Public ReadOnly Property InsertCommand() As SQLite.SQLiteCommand
            Get
                Return _InsertCommand.Clone
            End Get
        End Property

        Public ReadOnly Property SelectCommand() As SQLite.SQLiteCommand
            Get
                Return Me._SelectCommand.Clone
            End Get
        End Property

        Public ReadOnly Property SelectCountCommand() As SQLite.SQLiteCommand
            Get
                Return Me._SelectCountCommand.Clone
            End Get
        End Property

        Public ReadOnly Property SelectTombstoneCommand() As SQLite.SQLiteCommand
            Get
                Return Me._SelectTombstoneCommand.Clone
            End Get
        End Property

        Public ReadOnly Property PrimaryKeys() As ArrayList
            Get
                Return Me._PrimaryKeys
            End Get
        End Property

        Public ReadOnly Property CommonKeys() As ArrayList
            Get
                Return Me._CommonColumns
            End Get
        End Property

        Public ReadOnly Property WhereClause() As String
            Get
                Return Me._Where
            End Get
        End Property

        Public Sub Dispose() Implements System.IDisposable.Dispose
            If _InsertCommand IsNot Nothing Then
                _InsertCommand.Dispose()
            End If
            If _UpdateCommand IsNot Nothing Then
                _UpdateCommand.Dispose()
            End If
            If _DeleteCommand IsNot Nothing Then
                _DeleteCommand.Dispose()
            End If
            If _SelectCommand IsNot Nothing Then
                _SelectCommand.Dispose()
            End If
            If _SelectCountCommand IsNot Nothing Then
                _SelectCountCommand.Dispose()
            End If
            If _SelectTombstoneCommand IsNot Nothing Then
                _SelectTombstoneCommand.Dispose()
            End If

            _PrimaryKeys = Nothing
            _CommonColumns = Nothing
            _InsertCommand = Nothing
            _UpdateCommand = Nothing
            _DeleteCommand = Nothing
            _SelectCommand = Nothing
            _SelectCountCommand = Nothing
            _SelectTombstoneCommand = Nothing
            _SQLiteMetaDataHelper = Nothing
            _Connection = Nothing
            _Transaction = Nothing
        End Sub

    End Class


End Namespace
