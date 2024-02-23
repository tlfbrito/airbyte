/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.core.config

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import java.util.Optional

/**
 * Interface that defines a typed connector configuration.
 */
interface ConnectorConfiguration {
    fun getRawNamespace(): Optional<String>

    fun toJson(): JsonNode {
        return Jsons.jsonNode(this)
    }
}