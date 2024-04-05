/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.core.command.option

import io.airbyte.cdk.core.context.env.ConnectorConfigurationPropertySource
import io.airbyte.commons.json.Jsons
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog
import io.micronaut.context.annotation.ConfigurationProperties
import io.micronaut.context.annotation.Requires
import java.util.function.Supplier

interface ConfiguredAirbyteCatalogSupplier : Supplier<ConfiguredAirbyteCatalog>

/**
 * Micronaut configured properties holder for the Airbyte configured catalog provided to the
 * connector CLI as an argument.
 */
@ConfigurationProperties("airbyte.connector.catalog")
class ConfiguredAirbyteCatalogPOJO : ConfiguredAirbyteCatalogSupplier {

    var json: String = "{}"

    private val validated: ConfiguredAirbyteCatalog by lazy {
        Jsons.deserialize(json, ConfiguredAirbyteCatalog::class.java)
    }

    override fun get(): ConfiguredAirbyteCatalog = validated

}

