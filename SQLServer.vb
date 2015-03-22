Public Class SQLServer
    Implements IDisposable

    Private _connectionString As String = String.Empty
    Private _connection As System.Data.SqlClient.SqlConnection = Nothing
    Private _transaction As System.Data.SqlClient.SqlTransaction = Nothing
    Private _useTransaction As Boolean = False

    Public Sub setConnectionString(fingerPrint As String, connectionString As String, Optional useTransaction As Boolean = False)
        Me._connectionString = (New Polestar.Security.Cryptography).Decrypt(connectionString, fingerPrint)
        Me._useTransaction = useTransaction
        Me.Open()

    End Sub

    Public ReadOnly Property ConnectionString As String
        Get
            Return Me._connectionString
        End Get
    End Property

    Public Property Transaction As Boolean
        Get
            Return Me._useTransaction
        End Get
        Set(value As Boolean)
            Me._useTransaction = value
        End Set
    End Property

    Public Sub Open()

        If IsNothing(Me._connection) Then

            Me._connection = New System.Data.SqlClient.SqlConnection(Me.ConnectionString)
            Me._connection.Open()
            If Me.Transaction = True Then _transaction = Me._connection.BeginTransaction

        Else
            If _connection.State = ConnectionState.Open Then Return

            Me._connection = New System.Data.SqlClient.SqlConnection(Me.ConnectionString)
            Me._connection.Open()
            If Me.Transaction = True Then _transaction = Me._connection.BeginTransaction

        End If
        
    End Sub

    Public Sub Close()

        Me._connection.Close()

    End Sub

    Public Sub Rollback()
        If Me._useTransaction Then Me._transaction.Rollback()
    End Sub

    Public Sub Commit()
        If Me._useTransaction Then Me._transaction.Commit()
    End Sub

    Public Sub New()
        Me._connectionString = String.Empty
        Me._useTransaction = False
    End Sub

    Public Sub New(connectionString As String, Optional useTransaction As Boolean = False)
        Me._connectionString = connectionString
        Me._useTransaction = useTransaction
        Me.Open()
    End Sub

    Public Sub New(fingerPrint As String, connectionString As String, Optional useTransaction As Boolean = False)
        Me._connectionString = (New Polestar.Security.Cryptography).Decrypt(connectionString, fingerPrint)
        Me._useTransaction = useTransaction
        Me.Open()
    End Sub

    Public Sub Dispose() Implements IDisposable.Dispose
        Try
            If Me.Transaction Then
                If IsNothing(Me._transaction) Then Me._transaction.Rollback()
            End If

        Catch ex As Exception

        Finally
            Me._transaction.Dispose()
            Me._connection.Close()
            Me._connection.Dispose()

        End Try

    End Sub

    Public Function ExecuteQueryNoResult(sql As String, Optional fingerPrint As String = Nothing) As Long
        Dim cmd As New System.Data.SqlClient.SqlCommand

        If Me.Transaction Then
            If IsNothing(fingerPrint) Then
                cmd = New System.Data.SqlClient.SqlCommand(sql, Me._connection, Me._transaction)
            Else
                cmd = New System.Data.SqlClient.SqlCommand((New Polestar.Security.Cryptography).Decrypt(sql, fingerPrint), Me._connection, Me._transaction)
            End If


        Else
            If IsNothing(fingerPrint) Then
                cmd = New System.Data.SqlClient.SqlCommand(sql, Me._connection)
            Else
                cmd = New System.Data.SqlClient.SqlCommand((New Polestar.Security.Cryptography).Decrypt(sql, fingerPrint), Me._connection)
            End If

        End If
        Return cmd.ExecuteNonQuery()

    End Function

    Public Function ExecuteQuery(sql As String, Optional fingerPrint As String = Nothing) As System.Data.SqlClient.SqlDataReader
        Dim cmd As System.Data.SqlClient.SqlCommand

        If Me.Transaction Then
            If IsNothing(fingerPrint) Then
                cmd = New System.Data.SqlClient.SqlCommand(sql, Me._connection, Me._transaction)
            Else
                cmd = New System.Data.SqlClient.SqlCommand((New Polestar.Security.Cryptography).Decrypt(sql, fingerPrint), Me._connection, Me._transaction)
            End If


        Else
            If IsNothing(fingerPrint) Then
                cmd = New System.Data.SqlClient.SqlCommand(sql, Me._connection)
            Else
                cmd = New System.Data.SqlClient.SqlCommand((New Polestar.Security.Cryptography).Decrypt(sql, fingerPrint), Me._connection)
            End If

        End If

        Return cmd.ExecuteReader

    End Function

    Public Function DataReader2DataTable(reader As System.Data.SqlClient.SqlDataReader,
                                         Optional dataTableName As String = Nothing) As System.Data.DataTable

        Dim result As New System.Data.DataTable(dataTableName)
        result.Load(reader)

        Return result

    End Function

    ''' <summary>
    ''' 暗号化された、DataTableを Finger Printで復元する
    ''' 
    ''' </summary>
    ''' <param name="fingerPrint"></param>
    ''' <param name="src"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Function decryptDataTable(fingerPrint As String, src As System.Data.DataTable,
                                     Optional dataTableName As String = Nothing) As System.Data.DataTable

        Dim result As New System.Data.DataTable(dataTableName)

        '
        '   すべてのカラムを、文字型にして、Finger Printで複合化する
        '
        '   カラム名を定義
        For Each _column As System.Data.DataColumn In src.Columns
            result.Columns.Add(_column.ColumnName, GetType(String))
        Next

        '   row を暗号化
        For Each _row As System.Data.DataRow In src.Rows
            Dim row As System.Data.DataRow = result.NewRow
            For i As Integer = 0 To src.Columns.Count - 1
                If _row(i).Equals(DBNull.Value) Then
                Else
                    row(i) = (New Polestar.Security.Cryptography(fingerPrint)).Decrypt(_row(i))
                End If
            Next
            result.Rows.Add(row)
        Next

        Return result

    End Function

    ''' <summary>
    ''' Crypt DataTable
    ''' </summary>
    ''' <param name="fingerPrint"></param>
    ''' <param name="source"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Function cryptDataTable(fingerPrint As String, source As System.Data.DataTable,
                                   Optional dataTableName As String = Nothing) As System.Data.DataTable

        Dim result As New System.Data.DataTable(dataTableName)

        '
        '   すべてのカラムを、文字型にして、Finger Printで暗号化する
        '
        '   カラム名を定義
        For Each _column As System.Data.DataColumn In source.Columns
            result.Columns.Add(_column.ColumnName, GetType(String))
        Next

        '   row を暗号化
        For Each _row As System.Data.DataRow In source.Rows
            Dim row As System.Data.DataRow = result.NewRow
            For i As Integer = 0 To source.Columns.Count - 1
                If _row(i).Equals(DBNull.Value) Then

                Else
                    row(i) = (New Polestar.Security.Cryptography(fingerPrint)).Encrypt(_row(i))
                End If

            Next
            result.Rows.Add(row)
        Next

        Return result

    End Function

End Class
