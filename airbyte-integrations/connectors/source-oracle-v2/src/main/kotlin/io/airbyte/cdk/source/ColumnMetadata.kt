package io.airbyte.cdk.source

import java.sql.JDBCType

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
