/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.operation

import io.airbyte.commons.json.Jsons
import io.airbyte.commons.resources.MoreResources
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.function.Consumer

private val logger = KotlinLogging.logger {}

@Singleton
@Named("specOperation")
@Requires(property = CONNECTOR_OPERATION, value = "spec")
class DefaultSpecOperation(
    @Value("\${airbyte.connector.specification.file:spec.json}") private val specFile: String,
    @Named("outputRecordCollector") private val outputRecordCollector: Consumer<AirbyteMessage>
) : Operation {

    override val type = OperationType.SPEC

    override fun execute() {
        logger.info { "Using default spec operation." }
        val spec: ConnectorSpecification
        try {
            val specString: String = MoreResources.readResource(specFile)
            spec = Jsons.deserialize(specString, ConnectorSpecification::class.java)
        } catch (e: Exception) {
            throw OperationExecutionException("Failed to retrieve connector specification from resource '$specFile'.", e)
        }
        outputRecordCollector.accept(
            AirbyteMessage()
                .withType(AirbyteMessage.Type.SPEC)
                .withSpec(spec))
    }
}
