Namespace Tools
    Friend Class SQLiteTypeMapping

        Private _DbTypeXCreateTable As New Hashtable 'Of String, DbType
        Private _SupportedSqlTypes As New ArrayList

        Sub New()
            With Me._DbTypeXCreateTable
                .Add(DbType.Byte, "TINYINT")
                .Add(DbType.Int16, "SmallInt")
                .Add(DbType.Int32, "INT")
                .Add(DbType.Int64, "INTEGER")
                .Add(DbType.String, "VARCHAR")
                .Add(DbType.StringFixedLength, "CHAR")
                .Add(DbType.Double, "DOUBLE")
                .Add(DbType.Single, "REAL")
                .Add(DbType.Boolean, "BIT")
                .Add(DbType.Decimal, "DECIMAL")
                .Add(DbType.DateTime, "DATETIME")
                .Add(DbType.Binary, "BLOB")
                .Add(DbType.Guid, "GUID")
                .Add(DbType.Time, "TIME")
            End With
            With Me._SupportedSqlTypes
                .Add("int")
                .Add("smallint")
                .Add("bit")
                .Add("float")
                .Add("real")
                .Add("nvarchar")
                .Add("varchar")
                .Add("timestamp")
                .Add("varbinary")
                .Add("image")
                .Add("text")
                .Add("ntext")
                .Add("bigint")
                .Add("char")
                .Add("numeric")
                .Add("binary")
                .Add("smalldatetime")
                .Add("smallmoney")
                .Add("money")
                .Add("tinyint")
                .Add("uniqueidentifier")
                .Add("xml")
                .Add("sql_variant")
                .Add("decimal")
                .Add("nchar")
                .Add("datetime")
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

            SqlType = SqlType.ToLower()

            If Not Me._SupportedSqlTypes.Contains(SqlType) Then
                Return ""
            End If

            If SqlType = "timestamp" Then
                SqlType = "blob"
            ElseIf SqlType = "datetime" OrElse SqlType = "smalldatetime" Then
                SqlType = "timestamp"
            ElseIf SqlType = "decimal" Then
                SqlType = "numeric"
            ElseIf SqlType = "money" OrElse SqlType = "smallmoney" Then
                SqlType = "numeric"
            ElseIf SqlType = "binary" OrElse SqlType = "varbinary" OrElse SqlType = "image" Then
                SqlType = "blob"
            ElseIf SqlType = "tinyint" Then
                SqlType = "smallint"
            ElseIf SqlType = "bigint" Then
                SqlType = "integer"
            ElseIf SqlType = "sql_variant" Then
                SqlType = "blob"
            ElseIf SqlType = "xml" Then
                SqlType = "varchar"
            ElseIf SqlType = "uniqueidentifier" Then
                SqlType = "uniqueidentifier" '"GUID"
            ElseIf SqlType = "ntext" Then
                SqlType = "text"
            ElseIf SqlType = "nchar" Then
                SqlType = "char"
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

        Private Function GetCreateTableTypeFromDbType(ByVal Type As DbType) As String
            If Me._DbTypeXCreateTable.Contains(Type) Then
                Return CType(Me._DbTypeXCreateTable.Item(Type), String)
            Else
                Return ""
            End If
        End Function
    End Class
End Namespace