package io.airbyte.cdk.command

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.cdk.ssh.SshConnectionOptions
import io.airbyte.cdk.ssh.SshTunnelMethodConfiguration
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog
import java.util.function.Supplier


abstract class ConnectorConfigurationJsonObjectBase

interface ConnectorConfigurationJsonObjectSupplier<T : ConnectorConfigurationJsonObjectBase>
    : Supplier<T> {
    val valueClass: Class<T>
    val jsonSchema: JsonNode
}

/** Interface that defines a typed connector configuration. */
sealed interface ConnectorConfiguration {

    val realHost: String
    val realPort: Int
    val sshTunnel: SshTunnelMethodConfiguration
    val sshConnectionOptions: SshConnectionOptions
}

interface SourceConnectorConfiguration : ConnectorConfiguration {

    val expectedStateType: AirbyteStateMessage.AirbyteStateType

    val jdbcUrlFmt: String
    val jdbcProperties: Map<String, String>

    val schemas: List<String>
}


interface ConnectorConfigurationSupplier<T : ConnectorConfiguration> : Supplier<T>

interface ConfiguredAirbyteCatalogSupplier : Supplier<ConfiguredAirbyteCatalog>

interface ConnectorInputStateSupplier : Supplier<List<AirbyteStateMessage>>
