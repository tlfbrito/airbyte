/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.operation

import com.kjetland.jackson.jsonSchema.JsonSchemaConfig
import com.kjetland.jackson.jsonSchema.JsonSchemaDraft
import com.kjetland.jackson.jsonSchema.JsonSchemaGenerator
import io.airbyte.commons.jackson.MoreMappers
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.net.URI
import java.util.function.Consumer


private val logger = KotlinLogging.logger {}

@Singleton
@Named("specOperation")
@Requires(property = CONNECTOR_OPERATION, value = "spec")
class DefaultSpecOperation(
    @Value("\${airbyte.connector.documentationUrl}") val documentationUrl: String,
    @Value("\${airbyte.connector.configurationClass}") val configurationClassName: String,
    @Named("outputRecordCollector") private val outputRecordCollector: Consumer<AirbyteMessage>
) : Operation {

    override val type = OperationType.SPEC

    override fun execute() {
        logger.info { "Using default spec operation." }
        val spec = ConnectorSpecification()
        try {
            spec.documentationUrl = URI.create(documentationUrl)
        } catch (e: Exception) {
            logger.error(e) { "Invalid documentation URL '$documentationUrl'." }
            throw OperationExecutionException(
                "Failed to generate connector specification " +
                    "using documentation URL '$documentationUrl'.",
                e,
            )
        }
        try {
            val configurationClass: Class<*> = Class.forName(configurationClassName)
            spec.connectionSpecification = generator.generateJsonSchema(configurationClass)
        } catch (e: Exception) {
            logger.error(e) { "Invalid configuration class '$configurationClassName'." }
            throw OperationExecutionException(
                "Failed to generate connector specification " +
                    "using configuration class '$configurationClassName'.",
                e,
            )
        }
        outputRecordCollector.accept(
            AirbyteMessage()
                .withType(AirbyteMessage.Type.SPEC)
                .withSpec(spec),
        )
    }

    companion object {

        val config: JsonSchemaConfig = JsonSchemaConfig.vanillaJsonSchemaDraft4()
            .withJsonSchemaDraft(JsonSchemaDraft.DRAFT_07)
            .withFailOnUnknownProperties(false)

        val generator = JsonSchemaGenerator(MoreMappers.initMapper(), config)
    }
}
