package io.airbyte.integrations.source.oracle

import io.airbyte.cdk.core.operation.SourceOperations
import io.airbyte.cdk.core.operation.TableName
import io.micronaut.context.annotation.Primary
import jakarta.inject.Named

@Primary
class OracleSourceOperations : SourceOperations {

    override fun selectStarFromTableLimit0(table: TableName) =
        "SELECT * FROM ${table.name} WHERE ROWNUM < 1"
}
