/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.command

import io.airbyte.cdk.operation.Operation
import io.airbyte.commons.exceptions.ConfigErrorException
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.ConfigurationProperties
import io.micronaut.context.annotation.Requires
import io.micronaut.context.event.ApplicationEventListener
import io.micronaut.context.event.StartupEvent
import jakarta.inject.Singleton
import java.util.function.Supplier

private val logger = KotlinLogging.logger {}

const val CONNECTOR_STATE_PREFIX: String = "airbyte.connector.state"

interface ConnectorInputStateSupplier : Supplier<List<AirbyteStateMessage>>

@ConfigurationProperties(CONNECTOR_STATE_PREFIX)
class ConnectorInputStateSupplierImpl : ConnectorInputStateSupplier {

    var json: String = "[]"

    private val validated: List<AirbyteStateMessage> by lazy {
        val list: List<AirbyteStateMessage> =
            try {
                JsonParser.parse<List<AirbyteStateMessage>>(json)
            } catch (e: Exception) {
                try {
                    listOf(JsonParser.parse<AirbyteStateMessage>(json))
                } catch (_: Exception) {
                    throw e
                }
            }
        if (list.isEmpty()) {
            return@lazy listOf<AirbyteStateMessage>()
        }
        val type: AirbyteStateMessage.AirbyteStateType = list.first().type
        val isGlobal: Boolean =
            when (type) {
                AirbyteStateMessage.AirbyteStateType.GLOBAL -> true
                AirbyteStateMessage.AirbyteStateType.STREAM -> false
                else -> throw ConfigErrorException("unsupported state type $type")
            }
        val filtered: List<AirbyteStateMessage> = list.filter { it.type == type }
        if (filtered.size < list.size) {
            val n = list.size - filtered.size
            logger.warn { "discarded $n state message(s) not of type $type" }
        }
        if (isGlobal) {
            if (filtered.size > 1) {
                logger.warn { "discarded all but last global state message" }
            }
            return@lazy listOf(filtered.last())
        }
        val lastOfEachStream: List<AirbyteStateMessage> =
            filtered
                .groupingBy { it.stream.streamDescriptor }
                .reduce { _, _, msg -> msg }
                .values
                .toList()
        if (lastOfEachStream.size < filtered.size) {
            logger.warn { "discarded all but last stream state message for each stream descriptor" }
        }
        return@lazy lastOfEachStream
    }

    override fun get(): List<AirbyteStateMessage> = validated
}

@Singleton
@Requires(property = CONNECTOR_STATE_PREFIX)
class ConnectorInputStateValidator(
    private val operation: Operation,
    private val configSupplier: ConnectorConfigurationSupplier<out ConnectorConfiguration>,
    private val stateSupplier: ConnectorInputStateSupplier,
) : ApplicationEventListener<StartupEvent> {

    override fun onApplicationEvent(event: StartupEvent) {
        val state: List<AirbyteStateMessage> = stateSupplier.get()
        if (state.isEmpty()) {
            // Input state is always optional.
            return
        }
        val stateType: AirbyteStateMessage.AirbyteStateType = state.first().type
        logger.info { "valid $stateType input state present for ${operation.type.name}" }
        if (!operation.type.acceptsState) {
            logger.warn { "${operation.type.name} does not accept state" }
            return
        }
        val expectedStateType: AirbyteStateMessage.AirbyteStateType =
            sourceConfig().expectedStateType
        if (expectedStateType != stateType) {
            throw IllegalArgumentException(
                "$stateType input state incompatible with config, which requires $expectedStateType"
            )
        }
    }

    private fun sourceConfig(): SourceConnectorConfiguration =
        configSupplier.get() as SourceConnectorConfiguration
}
