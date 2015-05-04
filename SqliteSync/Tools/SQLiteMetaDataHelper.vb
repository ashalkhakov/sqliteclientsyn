Imports Microsoft.Synchronization.Data

Namespace Tools

    Public Enum AnchorType
        SentAnchor = 0
        ReceivedAnchor = 1
    End Enum

    Public Class SQLiteMetaDataHelper


        Private Const _AnchorTable As String = "__Anchor"
        Private Const _ClientGuidTable As String = "__ClientGuid"
        Private Const _SequenceTable As String = "__Sequence"
        Private Const _SentAnchorColumnName As String = "SentAnchor"
        Private Const _ReceivedAnchorColumnName As String = "ReceivedAnchor"
        Private Const _ClientGuidColumnName As String = "ClientGuid"

        Private _AutoGenerateIfNotExists As Boolean
        Private _Connection As SQLite.SQLiteConnection
        Private _Transaction As SQLite.SQLiteTransaction

        Sub New(ByVal Connection As SQLite.SQLiteConnection, _
                ByVal Transaction As SQLite.SQLiteTransaction, _
                ByVal AutoGenerateIfNotExists As Boolean)

            If Connection Is Nothing Then
                Throw New ArgumentException("Connection parameter could not be null.", "Connection")
            End If

            If Transaction Is Nothing Then
                Throw New ArgumentException("Transaction could not be null.", "Transaction")
            End If

            If Connection.State = ConnectionState.Closed Then
                Connection.Open()
            End If

            Me._AutoGenerateIfNotExists = AutoGenerateIfNotExists
            Me._Connection = Connection
            Me._Transaction = _Transaction

            If Me._AutoGenerateIfNotExists Then
                If Not TableExists(_AnchorTable) Then
                    CreateAnchorTable()
                End If
                If Not TableExists(_ClientGuidTable) Then
                    CreateClientGuidTable()
                End If
                If Not TableExists(_SequenceTable) Then
                    CreateSequenceTable()
                End If
            End If
        End Sub

        Private Sub CreateSequenceTable()

            Dim CreateSequenceTableDDL As String = String.Format("CREATE TABLE [{0}] ( " & _
                                                 "[{1}] LONG  NULL)", _
                                                 _SequenceTable, "Sequence")

            Using CreateSequenceTable As New SQLite.SQLiteCommand(CreateSequenceTableDDL, Me._Connection, Me._Transaction)
                CreateSequenceTable.ExecuteNonQuery()
            End Using

            Using InsertRecord As New SQLite.SQLiteCommand(String.Format("INSERT INTO [{0}] values(0)", _SequenceTable), Me._Connection, Me._Transaction)
                InsertRecord.ExecuteNonQuery()
            End Using
        End Sub

        Private Sub CreateAnchorTable()

            Dim CreateAnchorTableDDL As String = String.Format("CREATE TABLE [{0}] ( " & _
                                                 "[TableName] varchar(40)  PRIMARY KEY NOT NULL, " & _
                                                 "[{1}] VARCHAR(20)  NULL, " & _
                                                 "[{2}] VARCHAR(20)  NULL )", _
                                                 _AnchorTable, _SentAnchorColumnName, _ReceivedAnchorColumnName)

            Using CreateAnchorTableCMD As New SQLite.SQLiteCommand(CreateAnchorTableDDL, Me._Connection, Me._Transaction)
                CreateAnchorTableCMD.ExecuteNonQuery()
            End Using

        End Sub

        Private Sub CreateClientGuidTable()


            Dim CreateGuidTableDDL As String = String.Format("CREATE TABLE [{0}] ( " & _
                                                " [{1}] GUID PRIMARY KEY NOT NULL)", _
                                                _ClientGuidTable, _ClientGuidColumnName)

            Using CreateGuidTableCMD As New SQLite.SQLiteCommand(CreateGuidTableDDL, Me._Connection, Me._Transaction)
                CreateGuidTableCMD.ExecuteNonQuery()
            End Using
        End Sub

        Public Function TableExists(ByVal TableName As String) As Boolean
            Using cmdTableExists As New SQLite.SQLiteCommand("SELECT count(*) " & _
                                                             "FROM sqlite_master " & _
                                                             "WHERE type = 'table' " & _
                                                             "AND name = @TableName", Me._Connection, Me._Transaction)

                cmdTableExists.Parameters.Add("@TableName", DbType.String).Value = TableName
                Return CType(cmdTableExists.ExecuteScalar(), Integer) > 0
            End Using
        End Function

        ''' <summary>
        ''' Returns the client GUID.
        ''' </summary>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Function GetClientGuid() As Guid
            Dim ClientGuid As System.Guid = System.Guid.Empty

            Using GuidCommand As New SQLite.SQLiteCommand(Me._Connection)
                GuidCommand.Transaction = Me._Transaction
                GuidCommand.CommandText = String.Format("SELECT {0} FROM {1}", _ClientGuidColumnName, _ClientGuidTable)
                ClientGuid = CType(GuidCommand.ExecuteScalar, System.Guid)
            End Using


            'If Guid not exists, create new guid and save locally.
            If ClientGuid.Equals(System.Guid.Empty) Then
                ClientGuid = System.Guid.NewGuid
                Using GuidInsertCommand As New SQLite.SQLiteCommand(Me._Connection)
                    GuidInsertCommand.Transaction = Me._Transaction
                    GuidInsertCommand.CommandText = String.Format("INSERT INTO {0} values(@NewGuid)", _ClientGuidTable)
                    GuidInsertCommand.Parameters.Add("@NewGuid", DbType.Guid).Value = ClientGuid
                    GuidInsertCommand.ExecuteNonQuery()
                End Using
            End If

            Return ClientGuid
        End Function


        ''' <summary>
        ''' Get the anchor from the database.
        ''' </summary>
        ''' <param name="tableName"></param>
        ''' <param name="AnchorType"></param>
        ''' <returns>String of hex anchor value.</returns>
        ''' <remarks></remarks>
        Public Function GetAnchorValue(ByVal tableName As String, ByVal AnchorType As AnchorType) As Object
            Dim AnchorValue As Object = Nothing

            Using CMD As New SQLite.SQLiteCommand(_Connection)
                CMD.Transaction = _Transaction
                CMD.CommandText = String.Format("SELECT {0} FROM {1} WHERE TableName = @TableName", _
                                                IIf(AnchorType = Tools.AnchorType.SentAnchor, _SentAnchorColumnName, _ReceivedAnchorColumnName), _
                                                _AnchorTable)

                CMD.Parameters.Add("@TableName", DbType.String).Value = tableName
                AnchorValue = CMD.ExecuteScalar
            End Using

            If AnchorValue Is Nothing Then
                AnchorValue = DBNull.Value
            End If

            'If AnchorType = Tools.AnchorType.ReceivedAnchor Then
            If AnchorValue IsNot DBNull.Value Then
                If CType(AnchorValue, String).Contains("LNG") Then
                    'is long
                    Dim lngAnchorValue As Long = Long.Parse(CType(AnchorValue, String).Replace("LNG", ""))
                    Return lngAnchorValue
                Else
                    'is in bytes.
                    Return HexEncoding.GetBytes(CType(AnchorValue, String).Replace("HEX", ""), 0)
                End If
            End If
            'Else
            '    If AnchorValue IsNot DBNull.Value Then
            '        Return CType(AnchorValue, Int64)
            '    End If
            'End If
            Return DBNull.Value
        End Function

        ''' <summary>
        ''' Save the anchor to the database.
        ''' </summary>
        ''' <param name="tableName"></param>
        ''' <param name="AnchorType"></param>
        ''' <remarks></remarks>
        Public Sub SetAnchorValue(ByVal tableName As String, ByVal AnchorType As AnchorType, ByVal AnchorValue As Object)
            Using CMDUpdAnchor As New SQLite.SQLiteCommand(_Connection)
                CMDUpdAnchor.Transaction = _Transaction
                If AnchorType = Tools.AnchorType.SentAnchor Then
                    If Not TypeOf AnchorValue Is Int64 Then
                        Throw New ArgumentException("Sent anchor value must be int64.", "AnchorValue")
                    End If
                Else
                    If Not TypeOf AnchorValue Is Long And Not TypeOf AnchorValue Is Byte() Then
                        Throw New ArgumentException("Received anchor value must be long or byte().", "AnchorValue")
                    End If
                End If

                If Not ExistsAnchorForTable(tableName) Then
                    CMDUpdAnchor.CommandText = String.Format("INSERT INTO {1} (TableName,{0}) " & _
                                                             " values(@TableName, @Anchor)", _
                                                             IIf(AnchorType = Tools.AnchorType.SentAnchor, _SentAnchorColumnName, _ReceivedAnchorColumnName), _
                                                             _AnchorTable)
                Else
                    CMDUpdAnchor.CommandText = String.Format("UPDATE {1} " & _
                                                             " SET {0} =  @Anchor " & _
                                                             " WHERE TableName = @TableName", _
                                                             IIf(AnchorType = Tools.AnchorType.SentAnchor, _SentAnchorColumnName, _ReceivedAnchorColumnName), _
                                                             _AnchorTable)
                End If

                CMDUpdAnchor.Parameters.Add("@TableName", DbType.String).Value = tableName

                'Sql2000 and 2005 anchor values are byte()
                'Sql2008 anchor values are long
                'SQLITE save timespan.
                If TypeOf AnchorValue Is Byte() Then
                    CMDUpdAnchor.Parameters.Add("@Anchor", DbType.String).Value = "HEX" & HexEncoding.ToString(CType(AnchorValue, Byte()))
                ElseIf TypeOf AnchorValue Is Long Then
                    CMDUpdAnchor.Parameters.Add("@Anchor", DbType.String).Value = "LNG" & CType(AnchorValue, Long)
                ElseIf TypeOf AnchorValue Is DateTime Then
                    CMDUpdAnchor.Parameters.Add("@Anchor", DbType.Int64).Value = CType(AnchorValue, Int64)
                End If


                CMDUpdAnchor.ExecuteNonQuery()

            End Using
        End Sub


        Public Function getLastAnchor() As Int64
            Using CMDLastSequence As New SQLite.SQLiteCommand(String.Format("SELECT sequence FROM {0}", _SequenceTable), Me._Connection, Me._Transaction)
                Return CType(CMDLastSequence.ExecuteScalar, Int64)
            End Using
        End Function

        Private Function ExistsAnchorForTable(ByVal tableName As String) As Boolean
            Dim ExistsAnchor As Boolean = False

            Using CmdExistAnchor As New SQLite.SQLiteCommand("SELECT count(*) FROM " & _AnchorTable & " WHERE TableName = @TableNameParam", _Connection)
                CmdExistAnchor.Parameters.Add("@TableNameParam", DbType.String).Value = tableName
                ExistsAnchor = (CmdExistAnchor.ExecuteScalar = 1)
            End Using

            Return ExistsAnchor
        End Function
    End Class
End Namespace