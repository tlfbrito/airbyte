package io.airbyte.cdk.core.event

import io.airbyte.cdk.core.command.option.ConnectorConfigurationSupplier
import io.airbyte.cdk.core.operation.Operation
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import io.micronaut.context.event.ApplicationEventListener
import io.micronaut.context.event.StartupEvent
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

@Singleton
@Requires(bean = ConnectorConfigurationSupplier::class)
class ConnectorConfigurationValidator(
    private val operation: Operation,
    private val configSupplier: ConnectorConfigurationSupplier<*>
) : ApplicationEventListener<StartupEvent> {

    override fun onApplicationEvent(event: StartupEvent) {
        configSupplier.get() // Force evaluation and validation of configuration.
        logger.info { "valid connector configuration present for ${operation.type.name}" }
        if (!operation.type.requiresConfiguration) {
            logger.warn { "${operation.type.name} does not require state" }
        }
    }
}
