/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.core.context.env

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.cdk.core.operation.OperationType
import io.airbyte.cdk.integrations.base.JavaBaseConstants
import io.airbyte.commons.json.Jsons
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.env.MapPropertySource
import io.micronaut.core.cli.CommandLine
import java.io.File
import java.nio.file.Path
import java.util.*

private val logger = KotlinLogging.logger {}

/**
 * Custom Micronaut {@link PropertySource} that reads the command line arguments provided via the
 * connector CLI and turns them into configuration properties. This allows the arguments to be
 * injected into code that depends on them via Micronaut. <p /> This property source adds the
 * following properties to the configuration if the matching value is present in the CLI arguments:
 * <ul> <li><b>airbyte.connector.operation</b> - the operation argument (e.g. "check", "discover",
 * etc)</li> <li><b>airbyte.connector.catalog</b> - the Airbyte configured catalog as JSON read from
 * the configured catalog file path argument, if present.</li> <li><b>airbyte.connector.config</b> -
 * the normalized Airbyte connector configuration read from the configured connector configuration
 * file path argument, if present.</li> <li><b>airbyte.connector.state</b> - the Airbyte connector
 * state as JSON read from the state file path argument, if present.</li> </ol>
 */
class ConnectorConfigurationPropertySource(commandLine: CommandLine) :
    MapPropertySource("connector", resolveValues(commandLine)) {
    companion object {
        const val CONNECTOR_OPERATION: String = "airbyte.connector.operation"
        const val CONNECTOR_CONFIG_PREFIX: String = "airbyte.connector.config"
        const val CONNECTOR_CATALOG_PREFIX: String = "airbyte.connector.catalog"
        const val CONNECTOR_STATE_PREFIX: String = "airbyte.connector.state"

        private fun resolveValues(commandLine: CommandLine): Map<String, Any> {
            val ops: List<OperationType> = OperationType.entries.filter { commandLine.optionValue(it.name.lowercase()) != null }
            if (ops.isEmpty()) {
                throw IllegalArgumentException("Command line is missing an operation.")
            }
            if (ops.size > 1) {
                throw IllegalArgumentException("Command line has multiple operations: $ops")
            }
            val values: MutableMap<String, Any> = mutableMapOf()
            values[CONNECTOR_OPERATION] = ops.first().name.lowercase()
            for ((cliOptionKey, prefix) in mapOf(
                JavaBaseConstants.ARGS_CONFIG_KEY to CONNECTOR_CONFIG_PREFIX,
                JavaBaseConstants.ARGS_CATALOG_KEY to CONNECTOR_CATALOG_PREFIX,
                JavaBaseConstants.ARGS_STATE_KEY to CONNECTOR_STATE_PREFIX,
            )) {
                val cliOptionValue = commandLine.optionValue(cliOptionKey) as String?
                if (cliOptionValue.isNullOrBlank()) {
                    continue
                }
                val jsonFile: File = Path.of(cliOptionValue).toFile()
                if (!jsonFile.exists()) {
                    logger.warn { "File '$jsonFile' not found for '$cliOptionKey'." }
                    continue
                }
                val jsonString: String = jsonFile.readText()
                val maybeJson: Optional<JsonNode> = Jsons.tryDeserialize(jsonString)
                if (maybeJson.isEmpty) {
                    logger.warn { "Invalid JSON for '$cliOptionValue'." }
                    continue
                }
                val json: JsonNode = maybeJson.get()
                values["$prefix.json"] = Jsons.serialize(json)
            }
            return values
        }
    }
}
