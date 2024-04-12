package io.airbyte.cdk.jdbc

import io.airbyte.protocol.models.JsonSchemaType

interface SourceOperations {

    fun selectStarFromTableLimit0(table: TableName): String

    fun toAirbyteType(c: ColumnMetadata): JsonSchemaType
}
