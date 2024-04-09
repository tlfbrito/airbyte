/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.core.operation

import io.airbyte.cdk.core.command.ConnectorConfigurationSupplier
import io.airbyte.cdk.core.command.SourceConnectorConfiguration
import io.airbyte.protocol.models.Field
import io.airbyte.protocol.models.JsonSchemaType
import io.airbyte.protocol.models.v0.AirbyteCatalog
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteStream
import io.airbyte.protocol.models.v0.AirbyteStreamNameNamespacePair
import io.airbyte.protocol.models.v0.CatalogHelpers
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.sql.Connection
import java.sql.JDBCType
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.SQLException
import java.sql.Statement
import java.util.function.Consumer

private val logger = KotlinLogging.logger {}

@Singleton
@Named("discoverOperation")
@Requires(property = CONNECTOR_OPERATION, value = "discover")
@Requires(env = ["source"])
class DefaultDiscoverOperation(
    private val configSupplier: ConnectorConfigurationSupplier<SourceConnectorConfiguration>,
    private val sourceOperations: SourceOperations,
    @Named("outputRecordCollector") private val outputRecordCollector: Consumer<AirbyteMessage>
) : Operation {

    override val type = OperationType.DISCOVER

    override fun execute() {
        val config: SourceConnectorConfiguration = configSupplier.get()
        val discoveredStreams: List<DiscoveredStream>
        config.createConnection().use {
            discoveredStreams = discover(sourceOperations, it, config.schemas).toList()
        }
        val airbyteStreams: List<AirbyteStream> = discoveredStreams.map {
            CatalogHelpers.createAirbyteStream(
                it.fullyQualifiedName.name,
                it.fullyQualifiedName.namespace,
                it.fields
            ).withSourceDefinedPrimaryKey(it.primaryKeys)
        }
        outputRecordCollector.accept(
            AirbyteMessage()
                .withType(AirbyteMessage.Type.CATALOG)
                .withCatalog(AirbyteCatalog().withStreams(airbyteStreams)))
    }
}

data class DiscoveredStream(
    val fullyQualifiedName: AirbyteStreamNameNamespacePair,
    val fields: List<Field>,
    val primaryKeys: List<List<String>>,
)

@Named("sourceOperations")
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

fun discover(sops: SourceOperations, conn: Connection, schemas: List<String>): Sequence<DiscoveredStream> {
    return schemas.asSequence()
        .flatMap { queryTableNames(conn, it) }
        .map { discoverStream(sops, conn, it) }
}

private fun queryTableNames(conn: Connection, schema: String?): List<TableName> {
    logger.info { "Querying table names for catalog discovery." }
    try {
        val results = mutableListOf<TableName>()
        val rs: ResultSet = conn.metaData.getTables(null, schema, null, null)
        while (rs.next()) {
            val tableName = TableName(
                catalog = rs.getString("TABLE_CAT"),
                schema = rs.getString("TABLE_SCHEM"),
                name = rs.getString("TABLE_NAME"),
                type = rs.getString("TABLE_TYPE") ?: "",
            )
            results.add(tableName)
        }
        return results.sortedBy {
            "${it.catalog ?: ""}.${it.schema ?: ""}.${it.name}.${it.type}"
        }
    } catch (e: SQLException) {
        logger.info { "Failed to query table names (code: ${e.errorCode}; SQLState: ${e.sqlState})" }
        logger.debug(e) { "Table name discovery query failed with exception" }
        return listOf()
    }
}

private fun discoverStream(sops: SourceOperations, conn: Connection, table: TableName): DiscoveredStream {
    val sql: String = sops.selectStarFromTableLimit0(table)
    logger.info { "Querying $sql for catalog discovery." }
    val columnMetadata: List<ColumnMetadata>
    table.catalog?.let { conn.catalog = it }
    table.schema?.let { conn.schema = it }
    conn.createStatement().use { stmt: Statement ->
        columnMetadata = discoverColumnMetadata(stmt, sql)
    }

    logger.info { "Discovered ${columnMetadata.size} columns in $table." }
    return DiscoveredStream(
        AirbyteStreamNameNamespacePair(table.name, table.schema ?: table.catalog!!),
        columnMetadata.map { Field.of(it.name, sops.toAirbyteType(it)) },
        discoverPrimaryKeys(conn, table),
    )
}

private fun discoverColumnMetadata(stmt: Statement, sql: String): List<ColumnMetadata> {
    stmt.fetchSize = 1
    val meta: ResultSetMetaData
    try {
        meta = stmt.executeQuery(sql).metaData
    } catch (e: SQLException) {
        logger.info {
            "Query failed with code ${e.errorCode}, SQLState ${e.sqlState};" +
                " not adding table to discovered catalog"
        }
        logger.debug(e) { "Discovery query $sql failed with exception." }
        return listOf()
    }
    return (1..meta.columnCount).map {
        ColumnMetadata(
            name = meta.getColumnName(it),
            type = swallow { meta.getColumnType(it) }?.let { JDBCType.valueOf(it) },
            typeName = swallow { meta.getColumnTypeName(it) },
            klazz = swallow { meta.getColumnClassName(it) }?.let { Class.forName(it) },
            isAutoIncrement = swallow { meta.isAutoIncrement(it) },
            isCaseSensitive = swallow { meta.isCaseSensitive(it) },
            isSearchable = swallow { meta.isSearchable(it) },
            isCurrency = swallow { meta.isCurrency(it) },
            isNullable = when (swallow { meta.isNullable(it) }) {
                ResultSetMetaData.columnNoNulls -> false
                ResultSetMetaData.columnNullable -> true
                else -> null
            },
            isSigned = swallow { meta.isSigned(it) },
            displaySize = swallow { meta.getColumnDisplaySize(it) },
            precision = swallow { meta.getPrecision(it) },
            scale = swallow { meta.getScale(it) },
        )
    }
}

private fun <T> swallow(supplier: () -> T): T? {
    try {
        return supplier()
    } catch (e: SQLException) {
        logger.debug(e) { "Metadata query triggered exception, ignoring value" }
    }
    return null
}

private fun discoverPrimaryKeys(conn: Connection, table: TableName): List<List<String>> {
    logger.info { "Querying primary key metadata for $table for catalog discovery." }
    try {
        val primaryKeyResultSet: ResultSet =
            conn.metaData.getPrimaryKeys(table.catalog, table.schema, table.name)
        val pkMap = mutableMapOf<String?, MutableMap<Int, String>>()
        while (primaryKeyResultSet.next()) {
            val pkCol: String = primaryKeyResultSet.getString("COLUMN_NAME")
            val pkOrdinal: Int = primaryKeyResultSet.getInt("KEY_SEQ")
            val pkName: String? = primaryKeyResultSet.getString("PK_NAME")
            pkMap.putIfAbsent(pkName, mutableMapOf())
            pkMap[pkName]!![pkOrdinal] = pkCol
        }
        val pks: List<List<String>> = pkMap.keys.toList()
            .sortedBy { it ?: "" }
            .map { pkMap[it]!!.toSortedMap().values.toList() }
        logger.info { "Found ${pks.size} primary key(s)." }
        return pks
    } catch (e: SQLException) {
        logger.info { "Failed to query primary keys for $table: " +
            "code = ${e.errorCode}; SQLState = ${e.sqlState})" }
        logger.debug(e) { "Primary key discovery query failed with exception." }
        return listOf()
    }
}
data class TableName(
    val catalog: String?,
    val schema: String?,
    val name: String,
    val type: String
)

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
