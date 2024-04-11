/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.oracle

import io.airbyte.cdk.jdbc.SourceOperations
import io.airbyte.cdk.jdbc.TableName
import jakarta.inject.Singleton

@Singleton
class OracleSourceOperations : SourceOperations {

    override fun selectStarFromTableLimit0(table: TableName) =
        "SELECT * FROM ${table.name} WHERE ROWNUM < 1"
}
