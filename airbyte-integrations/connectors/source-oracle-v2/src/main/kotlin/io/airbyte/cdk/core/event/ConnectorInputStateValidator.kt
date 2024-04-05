package io.airbyte.cdk.core.event

import io.airbyte.cdk.core.command.option.ConnectorConfiguration
import io.airbyte.cdk.core.command.option.ConnectorConfigurationSupplier
import io.airbyte.cdk.core.command.option.ConnectorInputStateSupplier
import io.airbyte.cdk.core.command.option.SourceConnectorConfiguration
import io.airbyte.cdk.core.operation.Operation
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStateMessage.AirbyteStateType
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import io.micronaut.context.event.ApplicationEventListener
import io.micronaut.context.event.StartupEvent
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

/**
 * Event listener that validates the Airbyte configured catalog, if present. This listener is
 * executed on application start.
 */
@Singleton
@Requires(bean = ConnectorInputStateSupplier::class)
@Requires(env = ["source"])
class ConnectorInputStateValidator(
    private val operation: Operation,
    private val configSupplier: ConnectorConfigurationSupplier<out ConnectorConfiguration>,
    private val stateSupplier: ConnectorInputStateSupplier,
) : ApplicationEventListener<StartupEvent> {

    override fun onApplicationEvent(event: StartupEvent) {
        val state: List<AirbyteStateMessage> = stateSupplier.get()
        if (state.isEmpty()) {
            // Input state is always optional.
            return
        }
        val stateType: AirbyteStateType = state.first().type
        logger.info { "valid $stateType input state present for ${operation.type.name}" }
        if (!operation.type.acceptsState) {
            logger.warn { "${operation.type.name} does not accept state" }
            return
        }
        val expectedStateType: AirbyteStateType = sourceConfig().expectedStateType
        if (expectedStateType != stateType) {
            throw IllegalArgumentException(
                "$stateType input state incompatible with config, which requires $expectedStateType"
            )
        }
    }

    private fun sourceConfig(): SourceConnectorConfiguration =
        configSupplier.get() as SourceConnectorConfiguration
}
