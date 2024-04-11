/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.command

import com.fasterxml.jackson.databind.JsonNode
import com.kjetland.jackson.jsonSchema.JsonSchemaConfig
import com.kjetland.jackson.jsonSchema.JsonSchemaDraft
import com.kjetland.jackson.jsonSchema.JsonSchemaGenerator
import io.airbyte.commons.exceptions.ConfigErrorException
import io.airbyte.commons.jackson.MoreMappers
import io.airbyte.commons.json.Jsons
import io.airbyte.validation.json.JsonSchemaValidator

data object JsonParser {

    inline fun <reified T> parseList(json: String): List<T> {
        val jsonSchema: JsonNode = generator.generateJsonSchema(T::class.java)
        val jsonList: List<JsonNode>
        try {
            val tree: JsonNode = Jsons.deserialize(json)
            jsonList = if (tree.isArray) tree.toList() else listOf(tree)
        } catch (e: Exception) {
            throw ConfigErrorException("malformed json value while parsing for ${T::class}", e)
        }
        if (T::class == JsonNode::class) {
            return jsonList.map { it as T }
        }
        for (element in jsonList) {
            val validationFailures = JsonSchemaValidator().validate(jsonSchema, element)
            if (validationFailures.isNotEmpty()) {
                throw ConfigErrorException(
                    "${T::class} json schema violation: ${validationFailures.first()}"
                )
            }
        }
        return jsonList.map {
            try {
                Jsons.`object`(it, T::class.java)
            } catch (e: Exception) {
                throw ConfigErrorException("failed to map valid json to ${T::class} ", e)
            }
        }
    }

    inline fun <reified T> parse(json: String): T =
        parseList<T>(json).firstOrNull()
            ?: throw ConfigErrorException("missing json value while parsing for ${T::class}")

    val config: JsonSchemaConfig =
        JsonSchemaConfig.vanillaJsonSchemaDraft4()
            .withJsonSchemaDraft(JsonSchemaDraft.DRAFT_07)
            .withFailOnUnknownProperties(false)

    val generator = JsonSchemaGenerator(MoreMappers.initMapper(), config)
}
