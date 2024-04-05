package io.airbyte.integrations.source.oracle.operation.executor

import io.airbyte.cdk.core.command.option.ConnectorConfigurationSupplier
import io.airbyte.cdk.core.operation.CONNECTOR_OPERATION
import io.airbyte.cdk.core.operation.Operation
import io.airbyte.cdk.core.operation.OperationType
import io.airbyte.integrations.source.oracle.config.properties.OracleSourceConfiguration
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.micronaut.context.annotation.Primary
import io.micronaut.context.annotation.Requires
import jakarta.inject.Named
import jakarta.inject.Singleton

@Singleton
@Primary
@Named("checkOperationExecutor")
@Requires(property = CONNECTOR_OPERATION, value = "check")
@Requires(env = ["source"])
@Requires(notEnv = ["cloud"])
class OracleSourceCheckOperation(
    private val configSupplier: ConnectorConfigurationSupplier<OracleSourceConfiguration>
) : Operation {

    override val type = OperationType.CHECK

    override fun execute(): Result<Unit> {
        val config = configSupplier.get()
        println(config.toString())
        TODO()
    }
}
