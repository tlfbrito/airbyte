package io.airbyte.cdk.core.command.option

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.cdk.core.context.env.ConnectorConfigurationPropertySource
import io.airbyte.commons.json.Jsons
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.ConfigurationProperties
import io.micronaut.context.annotation.Requires
import java.util.function.Supplier

private val logger = KotlinLogging.logger {}

interface AirbyteConnectorStateSupplier : Supplier<List<AirbyteStateMessage>>

@ConfigurationProperties("airbyte.connector.state")
class AirbyteStateMessageListPOJO : AirbyteConnectorStateSupplier {

    var json: String = "[]"

    private val validated: List<AirbyteStateMessage> by lazy {
        var jsonNode: JsonNode = Jsons.deserialize(json)
        if (!jsonNode.isArray) {
            jsonNode = Jsons.arrayNode().apply { add(jsonNode) }
        }
        val typeReference = object : TypeReference<List<AirbyteStateMessage>>() {}
        val list: List<AirbyteStateMessage> = Jsons.`object`(jsonNode, typeReference)
        if (list.isEmpty()) {
            return@lazy listOf<AirbyteStateMessage>()
        }
        val type: AirbyteStateMessage.AirbyteStateType = list.first().type
        val isGlobal: Boolean = when (type) {
            AirbyteStateMessage.AirbyteStateType.GLOBAL -> true
            AirbyteStateMessage.AirbyteStateType.STREAM -> false
            else -> throw RuntimeException("unsupported state type $type")
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
        val lastOfEachStream: List<AirbyteStateMessage> = filtered
            .groupingBy { it.stream.streamDescriptor }
            .reduce { _, _, msg -> msg }
            .values.toList()
        if (lastOfEachStream.size < filtered.size) {
            logger.warn { "discarded all but last stream state message for each stream descriptor" }
        }
        return@lazy lastOfEachStream
    }

    override fun get(): List<AirbyteStateMessage> = validated

}
