/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.core.command.option

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import java.util.Optional

/** Interface that defines a typed connector configuration. */
interface ConnectorConfiguration {

    fun getRealHost(): String

    fun getRealPort(): Int

    fun getDefaultNamespace(): Optional<String>

    fun getRawNamespace(): Optional<String>

    fun toJson(): JsonNode =
        Jsons.jsonNode(this)
}
