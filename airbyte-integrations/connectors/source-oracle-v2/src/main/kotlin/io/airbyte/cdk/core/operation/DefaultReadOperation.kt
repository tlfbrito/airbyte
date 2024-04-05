/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.core.operation

import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import jakarta.inject.Named
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

@Singleton
@Named("readOperation")
@Requires(property = CONNECTOR_OPERATION, value = "read")
@Requires(env = ["source"])
class DefaultReadOperation : Operation {

    override val type = OperationType.READ

    override fun execute() {
        logger.info { "Using default read operation." }
    }
}
