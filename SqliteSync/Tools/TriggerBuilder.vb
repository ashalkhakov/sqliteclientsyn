Imports Microsoft.Synchronization.Data

Namespace Tools
    Friend Class TriggerBuilder
        Public Shared Function GetUpdateTrigger(ByVal TableName As String) As String

            Return String.Format("CREATE TRIGGER [ON_TBL_{0}_UPDATE] " & Environment.NewLine & _
                                "AFTER UPDATE ON [{0}] " & Environment.NewLine & _
                                "FOR EACH ROW " & Environment.NewLine & _
                                "BEGIN" & Environment.NewLine & _
                                "/*" & Environment.NewLine & _
                                "Trigger generated by SQLiteClientSync class." & Environment.NewLine & _
                                "This only affects client-side updates." & Environment.NewLine & _
                                "*/" & Environment.NewLine & _
                                "UPDATE [{0}]" & Environment.NewLine & _
                                "SET __sysChangeTxBsn = (SELECT Sequence + 1 FROM __Sequence)" & Environment.NewLine & _
                                "WHERE [{0}].rowid = OLD.rowid" & Environment.NewLine & _
                                "AND NOT (OLD.__sysInsertTxBsn IS NULL AND NOT NEW.__sysInsertTxBsn IS NULL) /* Prevent the execution caused by the insert trigger. */" & Environment.NewLine & _
                                "AND (NOT NEW.__sysChangeTxBsn = -1 OR NEW.__sysChangeTxBsn IS NULL) ; /* Prevent execution for updates that comes from server side */" & Environment.NewLine & _
                                "/* Restore the sysChangeTxBsn for updates that comes from server*/" & Environment.NewLine & _
                                "UPDATE [{0}]" & Environment.NewLine & _
                                "SET __sysChangeTxBsn = OLD.__sysChangeTxBsn" & Environment.NewLine & _
                                "WHERE [{0}].rowid = OLD.rowid" & Environment.NewLine & _
                                "AND NEW.__sysChangeTxBsn = -1;" & Environment.NewLine & _
                                "/*Update the sequence table*/" & Environment.NewLine & _
                                "UPDATE [__Sequence]" & Environment.NewLine & _
                                "SET Sequence = Sequence + 1" & Environment.NewLine & _
                                "WHERE NOT (OLD.__sysInsertTxBsn IS NULL AND NOT NEW.__sysInsertTxBsn IS NULL) /* Prevent the execution caused by the insert trigger. */" & Environment.NewLine & _
                                "AND (NOT NEW.__sysChangeTxBsn = -1 OR NEW.__sysChangeTxBsn IS NULL);" & Environment.NewLine & _
                                "End", TableName)

                
        End Function

        Public Shared Function GetInsertTrigger(ByVal TableName As String, ByVal Direction As SyncDirection) As String
            If Direction = SyncDirection.Bidirectional Then
                Return String.Format("CREATE TRIGGER [ON_TBL_{0}_INSERT] " & Environment.NewLine & _
                                    "AFTER INSERT ON [{0}] " & Environment.NewLine & _
                                    "FOR EACH ROW " & Environment.NewLine & _
                                    "BEGIN" & Environment.NewLine & Environment.NewLine & _
                                    "/*" & Environment.NewLine & _
                                    "Trigger generated by SQLiteClientSync class." & Environment.NewLine & _
                                    "This only affects client-side inserts." & Environment.NewLine & _
                                    "*/" & Environment.NewLine & _
                                    "UPDATE [{0}]" & Environment.NewLine & _
                                    "SET __sysInsertTxBsn = (SELECT Sequence + 1 FROM __Sequence)" & Environment.NewLine & _
                                    "WHERE [{0}].rowid  = NEW.rowid" & Environment.NewLine & _
                                    "AND COALESCE(__sysReceived, 0) = 0;" & Environment.NewLine & Environment.NewLine & _
                                    "UPDATE [__Sequence]" & Environment.NewLine & _
                                    "SET Sequence = Sequence + 1" & Environment.NewLine & _
                                    "WHERE COALESCE(NEW.__sysReceived, 0) = 0;" & Environment.NewLine & _
                                    "End;" & Environment.NewLine, TableName)

            Else
                Return String.Format("CREATE TRIGGER [ON_TBL_{0}_INSERT] " & Environment.NewLine & _
                                    "AFTER INSERT ON [{0}] " & Environment.NewLine & _
                                    "FOR EACH ROW " & Environment.NewLine & _
                                    "BEGIN" & Environment.NewLine & Environment.NewLine & _
                                    "/*" & Environment.NewLine & _
                                    "Trigger generated by SQLiteClientSync class." & Environment.NewLine & _
                                    "This only affects client-side inserts." & Environment.NewLine & _
                                    "*/" & Environment.NewLine & _
                                    "UPDATE [{0}]" & Environment.NewLine & _
                                    "SET __sysInsertTxBsn = (SELECT Sequence + 1 FROM __Sequence)" & Environment.NewLine & _
                                    "WHERE [{0}].rowid  = NEW.rowid;" & Environment.NewLine & Environment.NewLine & _
                                    "UPDATE [__Sequence]" & Environment.NewLine & _
                                    "SET Sequence = Sequence + 1;" & Environment.NewLine & _
                                    "End;" & Environment.NewLine, TableName)
            End If




        End Function

        Public Shared Function GetDeleteTrigger(ByVal TableName As String, ByVal Direction As SyncDirection) As String
            If Direction = SyncDirection.Bidirectional Then
                Return String.Format("CREATE TRIGGER [ON_TBL_{0}_DELETE] " & Environment.NewLine & _
                                        "BEFORE DELETE ON [{0}] " & Environment.NewLine & _
                                        "FOR EACH ROW " & Environment.NewLine & _
                                        "BEGIN " & Environment.NewLine & Environment.NewLine & _
                                        "/*Insert in tombstone*/" & Environment.NewLine & _
                                        "INSERT INTO [{0}_tombstone] " & Environment.NewLine & _
                                        "SELECT * FROM [{0}] " & Environment.NewLine & _
                                        "WHERE [{0}].rowid = old.rowid; " & Environment.NewLine & Environment.NewLine & _
                                        "/*Update the ChangeTxBSN*/ " & Environment.NewLine & _
                                        "UPDATE [{0}_tombstone] " & Environment.NewLine & _
                                        "SET __sysChangeTxBsn = (SELECT sequence + 1 FROM __Sequence), __sysReceived = 0" & Environment.NewLine & _
                                        "WHERE [{0}_tombstone].rowid = (SELECT max(rowid) FROM [{0}_tombstone]);" & Environment.NewLine & _
                                        "/*Update the sequence table*/" & Environment.NewLine & _
                                        "UPDATE [__Sequence]" & Environment.NewLine & _
                                        "SET Sequence = Sequence + 1;" & Environment.NewLine & _
                                        "End;", TableName)
            Else
                Return String.Format("CREATE TRIGGER [ON_TBL_{0}_DELETE] " & Environment.NewLine & _
                                        "BEFORE DELETE ON [{0}] " & Environment.NewLine & _
                                        "FOR EACH ROW " & Environment.NewLine & _
                                        "BEGIN " & Environment.NewLine & Environment.NewLine & _
                                        "/*Insert in tombstone*/" & Environment.NewLine & _
                                        "INSERT INTO [{0}_tombstone] " & Environment.NewLine & _
                                        "SELECT * FROM [{0}] " & Environment.NewLine & _
                                        "WHERE [{0}].rowid = old.rowid; " & Environment.NewLine & Environment.NewLine & _
                                        "/*Update the ChangeTxBSN*/ " & Environment.NewLine & _
                                        "UPDATE [{0}_tombstone] " & Environment.NewLine & _
                                        "SET __sysChangeTxBsn = (SELECT sequence + 1 FROM __Sequence)" & Environment.NewLine & _
                                        "WHERE [{0}_tombstone].rowid = (SELECT max(rowid) FROM [{0}_tombstone]);" & Environment.NewLine & _
                                        "/*Update the sequence table*/" & Environment.NewLine & _
                                        "UPDATE [__Sequence]" & Environment.NewLine & _
                                        "SET Sequence = Sequence + 1;" & Environment.NewLine & _
                                        "End;", TableName)
            End If



        End Function
    End Class
End Namespace