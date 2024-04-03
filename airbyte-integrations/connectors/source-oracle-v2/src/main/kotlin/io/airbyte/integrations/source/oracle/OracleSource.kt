package io.airbyte.integrations.source.oracle

import io.airbyte.cdk.core.IntegrationCommand
import io.airbyte.cdk.core.context.AirbyteConnectorRunner

fun main(args: Array<String>) {
    AirbyteConnectorRunner.run(IntegrationCommand::class.java, *args)
}
