package io.airbyte.cdk.jdbc

import java.sql.JDBCType

interface MetadataQuerier : AutoCloseable {

    fun tableNames(): List<TableName>
    fun columnMetadata(table: TableName, sql: String): List<ColumnMetadata>
    fun primaryKeys(table: TableName): List<List<String>>
}

data class TableName(val catalog: String?, val schema: String?, val name: String, val type: String)

data class ColumnMetadata(
    val name: String,
    val type: JDBCType?,
    val typeName: String?,
    val klazz: Class<*>?,
    val isAutoIncrement: Boolean?,
    val isCaseSensitive: Boolean?,
    val isSearchable: Boolean?,
    val isCurrency: Boolean?,
    val isNullable: Boolean?,
    val isSigned: Boolean?,
    val displaySize: Int?,
    val precision: Int?,
    val scale: Int?,
)
