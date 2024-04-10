/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.operation

import io.airbyte.cdk.source.ColumnMetadata
import io.airbyte.cdk.source.MetadataQuerier
import io.airbyte.cdk.source.SourceOperations
import io.airbyte.cdk.source.TableName
import io.airbyte.protocol.models.Field
import io.airbyte.protocol.models.v0.AirbyteCatalog
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteStream
import io.airbyte.protocol.models.v0.AirbyteStreamNameNamespacePair
import io.airbyte.protocol.models.v0.CatalogHelpers
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.sql.SQLException
import java.util.function.Consumer

private val logger = KotlinLogging.logger {}

@Singleton
@Named("discoverOperation")
@Requires(property = CONNECTOR_OPERATION, value = "discover")
@Requires(env = ["source"])
class DefaultDiscoverOperation(
    private val sourceOperations: SourceOperations,
    private val metadataQuerier: MetadataQuerier,
    @Named("outputRecordCollector") private val outputRecordCollector: Consumer<AirbyteMessage>
) : Operation, AutoCloseable {

    override val type = OperationType.DISCOVER

    override fun execute() {
        val airbyteStreams: List<AirbyteStream> =
            tableNames().mapNotNull(::discoveredStream).map {
                CatalogHelpers.createAirbyteStream(
                        it.fullyQualifiedName.name,
                        it.fullyQualifiedName.namespace,
                        it.fields
                    )
                    .withSourceDefinedPrimaryKey(it.primaryKeys)
            }
        outputRecordCollector.accept(
            AirbyteMessage()
                .withType(AirbyteMessage.Type.CATALOG)
                .withCatalog(AirbyteCatalog().withStreams(airbyteStreams))
        )
    }

    override fun close() {
        metadataQuerier
    }

    private fun tableNames(): List<TableName> {
        logger.info { "Querying table names for catalog discovery." }
        val tableNames: List<TableName>
        try {
            tableNames = metadataQuerier.tableNames()
        } catch (e: SQLException) {
            logger.info {
                "Failed to query table names; " + "code = ${e.errorCode}; SQLState = ${e.sqlState}"
            }
            logger.debug(e) { "Table name discovery query failed with exception." }
            return listOf()
        }
        logger.info { "Discovered ${tableNames.size} table(s)." }
        return tableNames
    }

    private fun discoveredStream(table: TableName): DiscoveredStream? {
        val sql: String = sourceOperations.selectStarFromTableLimit0(table)
        logger.info { "Querying $sql for catalog discovery." }
        val columnMetadata: List<ColumnMetadata>
        try {
            columnMetadata = metadataQuerier.columnMetadata(table, sql)
        } catch (e: SQLException) {
            logger.info {
                "Query failed with code ${e.errorCode}, SQLState ${e.sqlState};" +
                    " not adding table to discovered catalog."
            }
            logger.debug(e) { "Discovery query $sql failed with exception." }
            return null
        }
        logger.info { "Discovered ${columnMetadata.size} columns in $table." }
        if (columnMetadata.isEmpty()) {
            logger.info { "Skipping empty table." }
            return null
        }
        return DiscoveredStream(
            AirbyteStreamNameNamespacePair(table.name, table.schema ?: table.catalog!!),
            columnMetadata.map { Field.of(it.name, sourceOperations.toAirbyteType(it)) },
            discoverPrimaryKeys(table),
        )
    }

    data class DiscoveredStream(
        val fullyQualifiedName: AirbyteStreamNameNamespacePair,
        val fields: List<Field>,
        val primaryKeys: List<List<String>>,
    )

    private fun discoverPrimaryKeys(table: TableName): List<List<String>> {
        logger.info { "Querying primary key metadata for $table for catalog discovery." }
        val pks: List<List<String>>
        try {
            pks = metadataQuerier.primaryKeys(table)
        } catch (e: SQLException) {
            logger.info {
                "Failed to query primary keys for $table: " +
                    "code = ${e.errorCode}; SQLState = ${e.sqlState})"
            }
            logger.debug(e) { "Primary key discovery query failed with exception." }
            return listOf()
        }
        logger.info { "Found ${pks.size} primary key(s) in $table." }
        return pks
    }
}
