/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.command

import io.airbyte.cdk.operation.Operation
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.ConfigurationProperties
import io.micronaut.context.annotation.Requires
import io.micronaut.context.event.ApplicationEventListener
import io.micronaut.context.event.StartupEvent
import jakarta.inject.Singleton
import java.util.function.Supplier

private val logger = KotlinLogging.logger {}

const val CONNECTOR_CATALOG_PREFIX: String = "airbyte.connector.catalog"

interface ConfiguredAirbyteCatalogSupplier : Supplier<ConfiguredAirbyteCatalog>

@ConfigurationProperties(CONNECTOR_CATALOG_PREFIX)
class ConfiguredAirbyteCatalogSupplierImpl : ConfiguredAirbyteCatalogSupplier {

    var json: String = "{}"

    private val validated: ConfiguredAirbyteCatalog by lazy {
        JsonParser.parse<ConfiguredAirbyteCatalog>(json)
    }

    override fun get(): ConfiguredAirbyteCatalog = validated

}

@Singleton
@Requires(property = CONNECTOR_CATALOG_PREFIX)
class ConfiguredAirbyteCatalogValidator(
    private val operation: Operation,
    private val catalogSupplier: ConfiguredAirbyteCatalogSupplier,
) : ApplicationEventListener<StartupEvent> {

    override fun onApplicationEvent(event: StartupEvent) {
        val hasCatalog: Boolean = ConfiguredAirbyteCatalog() != catalogSupplier.get()
        if (operation.type.requiresCatalog && !hasCatalog) {
            throw IllegalArgumentException(
                "${operation.type.name} requires a valid configured catalog, none available")
        }
        if (hasCatalog) {
            logger.info { "valid configured catalog present for ${operation.type.name}" }
            if (!operation.type.requiresCatalog) {
                logger.warn { "${operation.type.name} does not require a catalog" }
            }
        }
    }
}
