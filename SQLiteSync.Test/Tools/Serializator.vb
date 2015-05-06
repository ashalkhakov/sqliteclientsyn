Imports System.IO
#If Not PocketPC Then
Imports System.Runtime.Serialization.Formatters.Binary
#End If
Imports System.Xml.Serialization

Public Enum SerializedFormat
    ''' <summary>
    ''' Binary serialization format.
    ''' </summary>
    Binary

    ''' <summary>
    ''' Document serialization format.
    ''' </summary>
    Document
End Enum

''' <summary>
''' Facade to XML serialization and deserialization of strongly typed objects to/from an XML file.
''' 
''' References: XML Serialization at http://samples.gotdotnet.com/:
''' http://samples.gotdotnet.com/QuickStart/howto/default.aspx?url=/quickstart/howto/doc/xmlserialization/rwobjfromxml.aspx
''' </summary>
Public NotInheritable Class Serializator(Of T As Class)
    Private Sub New()
    End Sub
    ' Specify that T must be a class.
#Region "Load methods"

    ''' <summary>
    ''' Loads an object from an XML file in Document format.
    ''' </summary>
    ''' <example>
    ''' <code>
    ''' serializableObject = ObjectXMLSerializer&lt;SerializableObject&gt;.Load(@"C:\XMLObjects.xml");
    ''' </code>
    ''' </example>
    ''' <param name="path">Path of the file to load the object from.</param>
    ''' <returns>Object loaded from an XML file in Document format.</returns>
    Public Shared Function Load(ByVal path As String) As T
        Dim serializableObject As T = LoadFromDocumentFormat(Nothing, path) 'Nothing
        Return serializableObject
    End Function

    ''' <summary>
    ''' Loads an object from an XML stream object.
    ''' </summary>
    ''' <example>
    ''' <code>
    ''' </code>
    ''' </example>
    ''' <param name="stream">Stream to load the object from.</param>
    ''' <returns>Object loaded from an XML file in Document format.</returns>
    Public Shared Function LoadFromStream(ByVal stream As System.IO.Stream, Optional ByVal extratypes As System.Type() = Nothing) As T
        Dim xmlSerializer As XmlSerializer = CreateXmlSerializer(extratypes)
        Dim serializableObject As T = xmlSerializer.Deserialize(stream)
        Return serializableObject
    End Function

    ''' <summary>
    ''' Loads an object from an XML file using a specified serialized format.
    ''' </summary>
    ''' <example>
    ''' <code>
    ''' serializableObject = ObjectXMLSerializer&lt;SerializableObject&gt;.Load(@"C:\XMLObjects.xml", SerializedFormat.Binary);
    ''' </code>
    ''' </example>		
    ''' <param name="path">Path of the file to load the object from.</param>
    ''' <param name="serializedFormat">XML serialized format used to load the object.</param>
    ''' <returns>Object loaded from an XML file using the specified serialized format.</returns>
    Public Shared Function Load(ByVal path As String, ByVal serializedFormat As SerializedFormat) As T
        Dim serializableObject As T = Nothing

        Select Case serializedFormat
            Case serializedFormat.Binary
#If PocketPC Then
                Throw New NotImplementedException("Load: binary not implemented")
#Else
                serializableObject = LoadFromBinaryFormat(path) 'Nothing
#End If
                Exit Select
            Case Else 'SerializedFormat.Document

                serializableObject = LoadFromDocumentFormat(Nothing, path) 'Nothing
                Exit Select
        End Select

        Return serializableObject
    End Function

    ''' <summary>
    ''' Loads an object from an XML file in Document format, supplying extra data types to enable deserialization of custom types within the object.
    ''' </summary>
    ''' <example>
    ''' <code>
    ''' serializableObject = ObjectXMLSerializer&lt;SerializableObject&gt;.Load(@"C:\XMLObjects.xml", new Type[] { typeof(MyCustomType) });
    ''' </code>
    ''' </example>
    ''' <param name="path">Path of the file to load the object from.</param>
    ''' <param name="extraTypes">Extra data types to enable deserialization of custom types within the object.</param>
    ''' <returns>Object loaded from an XML file in Document format.</returns>
    Public Shared Function Load(ByVal path As String, ByVal extraTypes As System.Type()) As T
        Dim serializableObject As T = LoadFromDocumentFormat(extraTypes, path) 'Nothing
        Return serializableObject
    End Function

#End Region

#Region "Save methods"


    Public Shared Sub SaveToStream(ByVal obj As T, ByVal Stream As System.IO.Stream)
        Dim xmlSerializer As XmlSerializer = CreateXmlSerializer(Nothing)
        xmlSerializer.Serialize(Stream, obj)
    End Sub

    ''' <summary>
    ''' Saves an object to an XML file in Document format.
    ''' </summary>
    ''' <example>
    ''' <code>        
    ''' SerializableObject serializableObject = new SerializableObject();
    ''' 
    ''' ObjectXMLSerializer&lt;SerializableObject&gt;.Save(serializableObject, @"C:\XMLObjects.xml");
    ''' </code>
    ''' </example>
    ''' <param name="serializableObject">Serializable object to be saved to file.</param>
    ''' <param name="path">Path of the file to save the object to.</param>
    Public Shared Sub Save(ByVal serializableObject As T, ByVal path As String)
        SaveToDocumentFormat(serializableObject, Nothing, path) 'Nothing
    End Sub

    ''' <summary>
    ''' Saves an object to an XML file using a specified serialized format.
    ''' </summary>
    ''' <example>
    ''' <code>
    ''' SerializableObject serializableObject = new SerializableObject();
    ''' 
    ''' ObjectXMLSerializer&lt;SerializableObject&gt;.Save(serializableObject, @"C:\XMLObjects.xml", SerializedFormat.Binary);
    ''' </code>
    ''' </example>
    ''' <param name="serializableObject">Serializable object to be saved to file.</param>
    ''' <param name="path">Path of the file to save the object to.</param>
    ''' <param name="serializedFormat">XML serialized format used to save the object.</param>
    Public Shared Sub Save(ByVal serializableObject As T, ByVal path As String, ByVal serializedFormat As SerializedFormat)
        Select Case serializedFormat
            Case serializedFormat.Binary
#If PocketPC Then
                Throw New NotSupportedException("Saving to binary format not implemented!")
#Else
                SaveToBinaryFormat(serializableObject, path) ' Nothing
#End If
                Exit Select
            Case Else ' SerializedFormat.Document, Else

                SaveToDocumentFormat(serializableObject, Nothing, path) 'Nothing
                Exit Select
        End Select
    End Sub

    ''' <summary>
    ''' Saves an object to an XML file in Document format, supplying extra data types to enable serialization of custom types within the object.
    ''' </summary>
    ''' <example>
    ''' <code>        
    ''' SerializableObject serializableObject = new SerializableObject();
    ''' 
    ''' ObjectXMLSerializer&lt;SerializableObject&gt;.Save(serializableObject, @"C:\XMLObjects.xml", new Type[] { typeof(MyCustomType) });
    ''' </code>
    ''' </example>
    ''' <param name="serializableObject">Serializable object to be saved to file.</param>
    ''' <param name="path">Path of the file to save the object to.</param>
    ''' <param name="extraTypes">Extra data types to enable serialization of custom types within the object.</param>
    Public Shared Sub Save(ByVal serializableObject As T, ByVal path As String, ByVal extraTypes As System.Type())
        SaveToDocumentFormat(serializableObject, extraTypes, path) 'Nothing
    End Sub

#End Region

#Region "Private"
    'ByVal isolatedStorageFolder As IsolatedStorageFile
    Private Shared Function CreateFileStream(ByVal path As String) As FileStream
        Dim fileStream As FileStream = Nothing

        'If isolatedStorageFolder Is Nothing Then
        fileStream = New FileStream(path, FileMode.OpenOrCreate)
        'Else
        'fileStream = New IsolatedStorageFileStream(path, FileMode.OpenOrCreate, isolatedStorageFolder)
        'End If

        Return fileStream
    End Function

#If Not PocketPC Then
    'ByVal isolatedStorageFolder As IsolatedStorageFile
    Private Shared Function LoadFromBinaryFormat(ByVal path As String) As T
        Dim serializableObject As T = Nothing

        'isolatedStorageFolder
        Using fileStream As FileStream = CreateFileStream(path)
            Dim binaryFormatter As New BinaryFormatter()
            serializableObject = TryCast(binaryFormatter.Deserialize(fileStream), T)
        End Using
        Return serializableObject
    End Function
#End If

    'ByVal isolatedStorageFolder As IsolatedStorageFile
    Private Shared Function LoadFromDocumentFormat(ByVal extraTypes As System.Type(), ByVal path As String) As T
        Dim serializableObject As T = Nothing
        Try

            'isolatedStorageFolder
            Using textReader As TextReader = CreateTextReader(path)
                Dim xmlSerializer As XmlSerializer = CreateXmlSerializer(extraTypes)
                serializableObject = TryCast(xmlSerializer.Deserialize(textReader), T)
            End Using
            Return serializableObject
        Catch ex As Exception
            Return Nothing
        End Try

    End Function

    'ByVal isolatedStorageFolder As IsolatedStorageFile
    Private Shared Function CreateTextReader(ByVal path As String) As TextReader
        Dim textReader As TextReader = Nothing

        'If isolatedStorageFolder Is Nothing Then
        textReader = New StreamReader(path)
        'Else
        'textReader = New StreamReader(New IsolatedStorageFileStream(path, FileMode.Open, isolatedStorageFolder))
        'End If

        Return textReader
    End Function

    'ByVal isolatedStorageFolder As IsolatedStorageFile
    Private Shared Function CreateTextWriter(ByVal path As String) As TextWriter
        Dim textWriter As TextWriter = Nothing

        'If isolatedStorageFolder Is Nothing Then
        textWriter = New StreamWriter(path)
        'Else
        'textWriter = New StreamWriter(New IsolatedStorageFileStream(path, FileMode.OpenOrCreate, isolatedStorageFolder))
        'End If

        Return textWriter
    End Function

    Private Shared Function CreateXmlSerializer(ByVal extraTypes As System.Type()) As XmlSerializer
        Dim ObjectType As Type = GetType(T)

        Dim xmlSerializer As XmlSerializer = Nothing

        If extraTypes IsNot Nothing Then
            xmlSerializer = New XmlSerializer(ObjectType, extraTypes)
        Else
            xmlSerializer = New XmlSerializer(ObjectType)
        End If

        Return xmlSerializer
    End Function

    'ByVal isolatedStorageFolder As IsolatedStorageFile
    Private Shared Sub SaveToDocumentFormat(ByVal serializableObject As T, ByVal extraTypes As System.Type(), ByVal path As String)
        'isolatedStorageFolder
        Using textWriter As TextWriter = CreateTextWriter(path)
            Dim xmlSerializer As XmlSerializer = CreateXmlSerializer(extraTypes)
            xmlSerializer.Serialize(textWriter, serializableObject)
        End Using
    End Sub

#If Not PocketPC Then
    'ByVal isolatedStorageFolder As IsolatedStorageFile
    Private Shared Sub SaveToBinaryFormat(ByVal serializableObject As T, ByVal path As String)
        'isolatedStorageFolder
        Using fileStream As FileStream = CreateFileStream(path)
            Dim binaryFormatter As New BinaryFormatter()
            binaryFormatter.Serialize(fileStream, serializableObject)
        End Using
    End Sub
#End If

#End Region
End Class
