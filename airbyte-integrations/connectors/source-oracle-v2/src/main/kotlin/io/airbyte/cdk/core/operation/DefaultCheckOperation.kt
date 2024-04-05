/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.core.operation

import io.airbyte.protocol.models.v0.AirbyteConnectionStatus
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.function.Consumer

private val logger = KotlinLogging.logger {}

@Singleton
@Named("checkOperation")
@Requires(property = CONNECTOR_OPERATION, value = "check")
class DefaultCheckOperation(
    @Named("outputRecordCollector") private val outputRecordCollector: Consumer<AirbyteMessage>
) : Operation {

    override val type = OperationType.CHECK

    override fun execute() {
        logger.info { "Using default check operation." }
        outputRecordCollector.accept(
            AirbyteMessage()
                .withType(AirbyteMessage.Type.CONNECTION_STATUS)
                .withConnectionStatus(AirbyteConnectionStatus()
                    .withStatus(AirbyteConnectionStatus.Status.SUCCEEDED)))
    }
}
