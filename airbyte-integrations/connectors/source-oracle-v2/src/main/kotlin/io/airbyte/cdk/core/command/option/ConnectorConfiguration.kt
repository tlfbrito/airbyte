/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.core.command.option

import io.airbyte.protocol.models.v0.AirbyteStateMessage.AirbyteStateType
import java.util.Optional
import java.util.function.Supplier

const val CONNECTOR_CONFIG_PREFIX: String = "airbyte.connector.config"

/** Interface that defines a typed connector configuration. */
interface ConnectorConfiguration {

    val realHost: String

    val realPort: Int

    fun getDefaultNamespace(): Optional<String>

}

interface ConnectorConfigurationSupplier<T : ConnectorConfiguration> : Supplier<T>

interface SourceConnectorConfiguration : ConnectorConfiguration {

    val expectedStateType: AirbyteStateType
}

interface DestinationConnectorConfiguration : ConnectorConfiguration {

    fun getRawNamespace(): Optional<String>

}
