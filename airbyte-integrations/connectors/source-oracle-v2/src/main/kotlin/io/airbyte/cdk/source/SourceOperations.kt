package io.airbyte.cdk.source

import io.airbyte.protocol.models.JsonSchemaType
import java.sql.JDBCType

interface SourceOperations {

    fun selectStarFromTableLimit0(table: TableName): String =
        "SELECT * FROM ${table.name} LIMIT 0"

    fun toAirbyteType(c: ColumnMetadata): JsonSchemaType = when (c.type) {
        JDBCType.BIT,
        JDBCType.BOOLEAN -> JsonSchemaType.BOOLEAN
        JDBCType.TINYINT,
        JDBCType.SMALLINT -> JsonSchemaType.INTEGER
        JDBCType.INTEGER -> JsonSchemaType.INTEGER
        JDBCType.BIGINT -> JsonSchemaType.INTEGER
        JDBCType.FLOAT,
        JDBCType.DOUBLE -> JsonSchemaType.NUMBER
        JDBCType.REAL -> JsonSchemaType.NUMBER
        JDBCType.NUMERIC,
        JDBCType.DECIMAL -> JsonSchemaType.NUMBER
        JDBCType.CHAR,
        JDBCType.NCHAR,
        JDBCType.NVARCHAR,
        JDBCType.VARCHAR,
        JDBCType.LONGVARCHAR -> JsonSchemaType.STRING
        JDBCType.DATE -> JsonSchemaType.STRING_DATE
        JDBCType.TIME -> JsonSchemaType.STRING_TIME_WITHOUT_TIMEZONE
        JDBCType.TIMESTAMP -> JsonSchemaType.STRING_TIMESTAMP_WITHOUT_TIMEZONE
        JDBCType.TIME_WITH_TIMEZONE -> JsonSchemaType.STRING_TIME_WITH_TIMEZONE
        JDBCType.TIMESTAMP_WITH_TIMEZONE -> JsonSchemaType.STRING_TIMESTAMP_WITH_TIMEZONE
        JDBCType.BLOB,
        JDBCType.BINARY,
        JDBCType.VARBINARY,
        JDBCType.LONGVARBINARY -> JsonSchemaType.STRING_BASE_64
        JDBCType.ARRAY -> JsonSchemaType.ARRAY
        else -> JsonSchemaType.STRING
    }
}
