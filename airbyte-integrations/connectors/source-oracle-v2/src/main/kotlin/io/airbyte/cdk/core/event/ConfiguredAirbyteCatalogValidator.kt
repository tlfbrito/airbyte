/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.core.event

import io.airbyte.cdk.core.command.option.ConfiguredAirbyteCatalogSupplier
import io.airbyte.cdk.core.operation.Operation
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import io.micronaut.context.event.ApplicationEventListener
import io.micronaut.context.event.StartupEvent
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

/**
 * Event listener that validates the Airbyte configured catalog, if present. This listener is
 * executed on application start.
 */
@Singleton
@Requires(bean = ConfiguredAirbyteCatalogSupplier::class)
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
