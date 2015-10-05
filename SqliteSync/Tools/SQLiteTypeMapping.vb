Namespace Tools
    Friend Class SQLiteTypeMapping

        Private _DbTypeXCreateTable As New Hashtable 'Of String, DbType
        Private _SupportedSqlTypes As New ArrayList

        Sub New()
            With Me._DbTypeXCreateTable
                .Add(DbType.Byte, "System.Byte")
                .Add(DbType.Int16, "System.Short")
                .Add(DbType.Int32, "System.Integer")
                .Add(DbType.Int64, "System.Long")
                .Add(DbType.String, "System.String")
                .Add(DbType.Double, "System.Double")
                .Add(DbType.Boolean, "System.Boolean")
                .Add(DbType.Decimal, "System.Decimal")
                .Add(DbType.Date, "System.Date")
                .Add(DbType.DateTime, "System.DateTime")
                .Add(DbType.Time, "System.Time")
                .Add(DbType.Binary, "System.Binary")
                '.Add(DbType.Binary, "System.Graphic") ' causes an error
                .Add(DbType.Guid, "System.Guid")
            End With
            With Me._SupportedSqlTypes
                .Add("System.Byte")
                .Add("System.Integer")
                .Add("System.Boolean")
                .Add("System.Double")
                .Add("System.Decimal")
                .Add("System.Time")
                .Add("System.Binary")
                .Add("System.Graphic")
                .Add("System.String")
                .Add("System.Decimal")
                .Add("System.Money")
                .Add("System.Short")
                .Add("System.Guid")
                .Add("System.DateTime")
                .Add("System.Date")
                .Add("System.UserID")
            End With

        End Sub

        Public Function GetCreateTableType(ByVal DataType As System.Type, ByVal ProviderType As String) As String
            Dim ConvertedFromProviderType As String
            ConvertedFromProviderType = GetCreateTableFromSqlServerType(ProviderType)
            If ConvertedFromProviderType <> "" Then
                Return ConvertedFromProviderType
            Else
                Dim ConvertedFromSystemType As String = GetCreateTableTypeFromDbType(GetDBType(DataType))
                If ConvertedFromSystemType <> "" Then
                    Return ConvertedFromSystemType
                Else
                    Throw New Exception(DataType.Name & " / " & ProviderType & " cannot be converted to sqlite column type.")
                End If
            End If
        End Function


        ''' <summary>
        ''' Get create table type from sql server provider type.
        ''' </summary>
        ''' <param name="SqlType"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Private Function GetCreateTableFromSqlServerType(ByVal SqlType As String) As String
            If Not Me._SupportedSqlTypes.Contains(SqlType) Then
                Return ""
            End If

            If SqlType = "System.Time" Then
                SqlType = "timestamp"
            ElseIf SqlType = "System.DateTime" OrElse SqlType = "Date" Then
                SqlType = "timestamp"
            ElseIf SqlType = "System.Decimal" Then
                SqlType = "numeric"
            ElseIf SqlType = "System.Money" Then
                SqlType = "numeric"
            ElseIf SqlType = "System.Binary" OrElse SqlType = "System.Graphic" Then
                SqlType = "blob"
            ElseIf SqlType = "System.Byte" Then
                SqlType = "tinyint"
            ElseIf SqlType = "System.Long" Then
                SqlType = "bigint"
            ElseIf SqlType = "System.Guid" Then
                SqlType = "uniqueidentifier" '"GUID"
            ElseIf SqlType = "System.String" Then
                SqlType = "text"
            ElseIf SqlType = "System.Boolean" Then
                SqlType = "char"
            ElseIf SqlType = "System.Double" Then
                SqlType = "float"
            ElseIf SqlType = "System.Short" Then
                SqlType = "smallint"
            ElseIf SqlType = "System.Integer" Then
                SqlType = "integer"
            ElseIf SqlType = "System.UserID" Then ' sigh
                SqlType = "text"
            End If

            Return SqlType
        End Function

        ''' <summary>
        ''' Converts CLR TYPE into DbType.
        ''' </summary>
        ''' <param name="theType"></param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Private Function GetDBType(ByVal theType As System.Type) As DbType
            Dim p1 As SQLite.SQLiteParameter
            'Dim tc As System.ComponentModel.TypeConverter
            'try to convert from theType to some DbType...
            'NOTE: I am not sure if this matches what ADO.NET does or not
            If theType.Equals(GetType(System.String)) Then
                Return DbType.String
            ElseIf theType.Equals(GetType(System.Boolean)) Then
                Return DbType.Boolean
                ' System.Binary does not exist in CF?
            ElseIf theType.Equals(GetType(System.Byte)) Then
                Return DbType.Byte
            ElseIf theType.Equals(GetType(System.Int32)) Then
                Return DbType.Int32
            ElseIf theType.Equals(GetType(System.Int16)) Then
                Return DbType.Int16
            ElseIf theType.Equals(GetType(System.Int64)) Then
                Return DbType.Int64
            ElseIf theType.Equals(GetType(System.Decimal)) Then
                Return DbType.Decimal
            ElseIf theType.Equals(GetType(System.Single)) Then
                Return DbType.Single
            ElseIf theType.Equals(GetType(System.Double)) Then
                Return DbType.Double
            ElseIf theType.Equals(GetType(System.String)) Then
                Return DbType.String
            ElseIf theType.Equals(GetType(System.DateTime)) Then
                Return DbType.DateTime
            ElseIf theType.Equals(GetType(System.Guid)) Then
                Return DbType.Guid
            ElseIf theType.Equals(GetType(Byte())) Then
                Return DbType.Binary
            End If
            p1 = New SQLite.SQLiteParameter()
            'tc = System.ComponentModel.TypeDescriptor.GetConverter(p1.DbType)
            'If tc.CanConvertFrom(theType) Then
            '    p1.DbType = tc.ConvertFrom(theType.Name)
            'Else
            '    'Try brute force
            '    Try
            '        p1.DbType = tc.ConvertFrom(theType.Name)
            '    Catch ex As Exception
            '        'Do Nothing
            '    End Try
            'End If
            Return p1.DbType
        End Function

        Public Function GetDbTypeFromProviderType(ByVal ProviderType As String) As DbType
            If ProviderType = "System.Byte" Then
                Return DbType.Byte
            ElseIf ProviderType = "System.Short" Then
                Return DbType.Int16
            ElseIf ProviderType = "System.Integer" Then
                Return DbType.Int32
            ElseIf ProviderType = "System.Long" Then
                Return DbType.Int64
            ElseIf ProviderType = "System.String" Then
                Return DbType.String
            ElseIf ProviderType = "System.Double" Then
                Return DbType.Double
            ElseIf ProviderType = "System.Boolean" Then
                Return DbType.Boolean
            ElseIf ProviderType = "System.Decimal" Then
                Return DbType.Decimal
            ElseIf ProviderType = "System.Date" Then
                Return DbType.Date
            ElseIf ProviderType = "System.DateTime" Then
                Return DbType.DateTime
            ElseIf ProviderType = "System.Time" Then
                Return DbType.Time
            ElseIf ProviderType = "System.Binary" Then
                Return DbType.Binary
            ElseIf ProviderType = "System.Guid" Then
                Return DbType.Guid
            ElseIf ProviderType = "System.UserID" Then
                Return DbType.String
            Else
                Return DbType.Binary
            End If
        End Function

        Private Function GetCreateTableTypeFromDbType(ByVal Type As DbType) As String
            If Me._DbTypeXCreateTable.Contains(Type) Then
                Return CType(Me._DbTypeXCreateTable.Item(Type), String)
            Else
                Return ""
            End If
        End Function
    End Class
End Namespace