/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.core.operation

import io.airbyte.protocol.models.v0.AirbyteMessage

const val CONNECTOR_OPERATION: String = "airbyte.connector.operation"

/**
 * Interface that defines a CLI operation. Each operation maps to one of the available {@link
 * OperationType}s and proxies to an {@link OperationExecutor} that performs the actual work.
 */
interface Operation {

    val type: OperationType

    fun execute(): Result<Unit>
}

/**
 * Defines the operations that may be invoked via the CLI arguments. Not all connectors will
 * implement all of these operations.
 */
enum class OperationType(
    val requiresConfiguration: Boolean = false,
    val requiresCatalog: Boolean = false,
    val acceptsState: Boolean = false,
) {
    SPEC,
    CHECK(requiresConfiguration = true),
    DISCOVER(requiresConfiguration = true),
    READ(requiresConfiguration = true, requiresCatalog = true, acceptsState = true),
    WRITE(requiresConfiguration = true, requiresCatalog = true),
}

/** Custom exception that represents a failure to execute an operation. */
class OperationExecutionException(message: String, cause: Throwable) : Exception(message, cause)
