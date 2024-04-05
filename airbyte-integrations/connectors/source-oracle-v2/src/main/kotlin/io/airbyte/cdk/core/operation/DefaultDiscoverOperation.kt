/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.core.operation

import io.airbyte.protocol.models.v0.AirbyteCatalog
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.function.Consumer

private val logger = KotlinLogging.logger {}

@Singleton
@Named("discoverOperation")
@Requires(property = CONNECTOR_OPERATION, value = "discover")
@Requires(env = ["source"])
class DefaultDiscoverOperation(
    @Named("outputRecordCollector") private val outputRecordCollector: Consumer<AirbyteMessage>
) : Operation {

    override val type = OperationType.DISCOVER

    override fun execute(): Result<Unit> {
        logger.info { "Using default discover operation." }
        outputRecordCollector.accept(
            AirbyteMessage()
                .withType(AirbyteMessage.Type.CATALOG)
                .withCatalog(AirbyteCatalog()))
        return Result.success(Unit)
    }
}
