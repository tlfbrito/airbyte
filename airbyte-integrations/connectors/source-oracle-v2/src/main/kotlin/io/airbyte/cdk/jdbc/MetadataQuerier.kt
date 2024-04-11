package io.airbyte.cdk.jdbc

interface MetadataQuerier : AutoCloseable {

    fun tableNames(): List<TableName>
    fun columnMetadata(table: TableName, sql: String): List<ColumnMetadata>
    fun primaryKeys(table: TableName): List<List<String>>
}
