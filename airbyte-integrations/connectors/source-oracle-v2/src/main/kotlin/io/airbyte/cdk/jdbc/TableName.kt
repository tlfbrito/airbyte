package io.airbyte.cdk.jdbc

data class TableName(val catalog: String?, val schema: String?, val name: String, val type: String)
