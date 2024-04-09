/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.core.operation

import io.airbyte.cdk.core.command.ConnectorConfigurationSupplier
import io.airbyte.cdk.core.command.SourceConnectorConfiguration
import io.airbyte.protocol.models.v0.AirbyteConnectionStatus
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.micronaut.context.annotation.Requires
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.sql.Connection
import java.sql.SQLException
import java.util.function.Consumer

@Singleton
@Named("checkOperation")
@Requires(property = CONNECTOR_OPERATION, value = "check")
class DefaultCheckOperation(
    private val configSupplier: ConnectorConfigurationSupplier<SourceConnectorConfiguration>,
    private val sourceOperations: SourceOperations,
    @Named("outputRecordCollector") private val outputRecordCollector: Consumer<AirbyteMessage>
) : Operation {

    override val type = OperationType.CHECK


    override fun execute() {
        outputRecordCollector.accept(
            AirbyteMessage()
                .withType(AirbyteMessage.Type.CONNECTION_STATUS)
                .withConnectionStatus(determineStatus()))
    }

    private fun determineStatus(): AirbyteConnectionStatus {
        try {
            val config = configSupplier.get()
            config.createConnection().use { conn: Connection ->
                // Don't query more than one stream successfully.
                discover(sourceOperations, conn, config.schemas)
                    .filter { it.fields.isNotEmpty() }
                    .take(1)
                    .forEach { _ -> }
            }
            return AirbyteConnectionStatus().withStatus(AirbyteConnectionStatus.Status.SUCCEEDED)
        } catch (e: SQLException) {
            val message: String = listOfNotNull(
                e.sqlState?.let { "State code: $it" },
                e.errorCode.takeIf { it != 0 }?.let { "Error code: $it" },
                e.message?.let { "Message : $it" },
            ).joinToString(separator = "; ")
            return AirbyteConnectionStatus()
                .withStatus(AirbyteConnectionStatus.Status.FAILED)
                .withMessage(message)
        } catch (e: Exception) {
            return AirbyteConnectionStatus()
                .withStatus(AirbyteConnectionStatus.Status.FAILED)
                .withMessage(e.message)
        }
    }
}
