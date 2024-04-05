package io.airbyte.integrations.source.oracle.operation.executor

import io.airbyte.cdk.core.context.env.ConnectorConfigurationPropertySource
import io.airbyte.cdk.core.operation.executor.OperationExecutor
import io.airbyte.integrations.source.oracle.config.properties.OracleSourceConfiguration
import io.airbyte.integrations.source.oracle.config.properties.OracleSourceConfigurationSupplier
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.micronaut.context.annotation.Primary
import io.micronaut.context.annotation.Requires
import jakarta.inject.Named
import jakarta.inject.Singleton

@Singleton
@Primary
@Named("checkOperationExecutor")
@Requires(
    property = ConnectorConfigurationPropertySource.CONNECTOR_OPERATION,
    value = "check",
)
@Requires(notEnv = ["cloud"])
class OracleSourceCheckOperationExecutor(private val configurationSupplier: OracleSourceConfigurationSupplier) : OperationExecutor {

    override fun execute(): Result<Sequence<AirbyteMessage>> {
        val configuration = configurationSupplier.get()
        println(configuration.toString())
        TODO()
    }
}
