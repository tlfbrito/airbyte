package io.airbyte.integrations.source.oracle

import io.airbyte.cdk.source.SourceOperations
import io.airbyte.cdk.source.TableName
import io.micronaut.context.annotation.Primary
import jakarta.inject.Singleton

@Singleton
@Primary
class OracleSourceOperations : SourceOperations {

    override fun selectStarFromTableLimit0(table: TableName) =
        "SELECT * FROM ${table.name} WHERE ROWNUM < 1"
}
