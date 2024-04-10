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

    inline fun <reified T> parse(json: String): T {
        val jsonSchema: JsonNode = generator.generateJsonSchema(T::class.java)
        val tree: JsonNode
        try {
            tree = Jsons.deserialize(json)
        } catch (e: Exception) {
            throw ConfigErrorException("malformed json value while parsing for ${T::class}", e)
        }
        if (T::class == JsonNode::class) {
            return tree as T
        }
        val validationFailures = JsonSchemaValidator().validate(jsonSchema, tree)
        if (validationFailures.isNotEmpty()) {
            throw ConfigErrorException(
                "${T::class} json schema violation: ${validationFailures.first()}"
            )
        }
        val result: T
        try {
            result = Jsons.`object`(tree, T::class.java)
        } catch (e: Exception) {
            throw ConfigErrorException("failed to map valid json to ${T::class} ", e)
        }
        return result
    }

    val config: JsonSchemaConfig =
        JsonSchemaConfig.vanillaJsonSchemaDraft4()
            .withJsonSchemaDraft(JsonSchemaDraft.DRAFT_07)
            .withFailOnUnknownProperties(false)

    val generator = JsonSchemaGenerator(MoreMappers.initMapper(), config)
}
