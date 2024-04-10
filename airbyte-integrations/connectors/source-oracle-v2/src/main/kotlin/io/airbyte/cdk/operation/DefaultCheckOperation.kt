/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.operation

import io.airbyte.cdk.source.MetadataQuerier
import io.airbyte.cdk.source.SourceOperations
import io.airbyte.cdk.source.TableName
import io.airbyte.protocol.models.v0.AirbyteConnectionStatus
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.sql.SQLException
import java.util.function.Consumer

private val logger = KotlinLogging.logger {}

@Singleton
@Named("checkOperation")
@Requires(property = CONNECTOR_OPERATION, value = "check")
class DefaultCheckOperation(
    private val sourceOperations: SourceOperations,
    private val metadataQuerier: MetadataQuerier,
    @Named("outputRecordCollector") private val outputRecordCollector: Consumer<AirbyteMessage>
) : Operation, AutoCloseable {

    override val type = OperationType.CHECK

    override fun execute() {
        outputRecordCollector.accept(
            AirbyteMessage()
                .withType(AirbyteMessage.Type.CONNECTION_STATUS)
                .withConnectionStatus(doCheck())
        )
    }

    override fun close() {
        metadataQuerier.close()
    }

    private fun doCheck(): AirbyteConnectionStatus {
        val tableNames: List<TableName>
        logger.info { "Connecting for config check, querying all table names in config schemas." }
        try {
            tableNames = metadataQuerier.tableNames()
        } catch (e: SQLException) {
            val message: String =
                listOfNotNull(
                        e.sqlState?.let { "State code: $it" },
                        e.errorCode.takeIf { it != 0 }?.let { "Error code: $it" },
                        e.message?.let { "Message : $it" },
                    )
                    .joinToString(separator = "; ")
            logger.debug(e) { "Table name discovery query failed with exception." }
            return AirbyteConnectionStatus()
                .withStatus(AirbyteConnectionStatus.Status.FAILED)
                .withMessage(message)
        }
        logger.info { "Discovered ${tableNames.size} table(s)." }
        for (table in tableNames) {
            val sql: String = sourceOperations.selectStarFromTableLimit0(table)
            logger.info { "Querying $sql for config check." }
            try {
                metadataQuerier.columnMetadata(table, sql)
            } catch (e: SQLException) {
                logger.info {
                    "Query failed with code ${e.errorCode}, SQLState ${e.sqlState};" +
                        " will try to query another table instead."
                }
                logger.debug(e) { "Config check query $sql failed with exception." }
                continue
            }
            logger.info { "Query successful." }
            return AirbyteConnectionStatus().withStatus(AirbyteConnectionStatus.Status.SUCCEEDED)
        }
        return AirbyteConnectionStatus()
            .withStatus(AirbyteConnectionStatus.Status.FAILED)
            .withMessage("Unable to query any of the discovered table(s): $tableNames")
    }
}
