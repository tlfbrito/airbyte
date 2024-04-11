/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.command

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.NullNode
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.kjetland.jackson.jsonSchema.JsonSchemaConfig
import com.kjetland.jackson.jsonSchema.JsonSchemaDraft
import com.kjetland.jackson.jsonSchema.JsonSchemaGenerator
import com.networknt.schema.JsonMetaSchema
import com.networknt.schema.JsonSchema
import com.networknt.schema.JsonSchemaFactory
import com.networknt.schema.SchemaLocation
import com.networknt.schema.SchemaValidatorsConfig
import com.networknt.schema.SpecVersion
import com.networknt.schema.ValidationContext
import io.airbyte.commons.exceptions.ConfigErrorException
import io.airbyte.commons.jackson.MoreMappers
import io.airbyte.validation.json.JsonSchemaValidator
import io.micronaut.retry.annotation.Fallback
import java.net.URI

data object JsonParser {

    inline fun <reified T> parseList(json: String): List<T> =
        parseList(json, T::class.java)

    inline fun <reified T> parse(json: String): T =
        parseList(json, T::class.java).firstOrNull()
            ?: throw ConfigErrorException("missing json value while parsing for ${T::class}")

    inline fun <reified T> parse(json: String?, fallback: T): T =
        parse(json ?: mapper.writeValueAsString(fallback))

    fun <T> parseList(json: String, klazz: Class<T>): List<T> {
        val jsonList: List<JsonNode>
        try {
            val tree: JsonNode = mapper.readTree(json)
            jsonList = if (tree.isArray) tree.toList() else listOf(tree)
        } catch (e: Exception) {
            throw ConfigErrorException("malformed json value while parsing for $klazz", e)
        }
        if (klazz != JsonNode::class.java) {
            val schemaNode: JsonNode = generator.generateJsonSchema(klazz)
            val config = SchemaValidatorsConfig()
            val jsonSchema: JsonSchema = jsonSchemaFactory.getSchema(schemaNode, config)
            for (element in jsonList) {
                val validationFailures = jsonSchema.validate(element)
                if (validationFailures.isNotEmpty()) {
                    throw ConfigErrorException(
                        "$klazz json schema violation: ${validationFailures.first()}"
                    )
                }
            }
        }
        return jsonList.map {
            try {
                mapper.treeToValue(it, klazz)
            } catch (e: Exception) {
                throw ConfigErrorException("failed to map valid json to $klazz ", e)
            }
        }
    }

    val config: JsonSchemaConfig =
        JsonSchemaConfig.vanillaJsonSchemaDraft4()
            .withJsonSchemaDraft(JsonSchemaDraft.DRAFT_07)
            .withFailOnUnknownProperties(false)

    val generator = JsonSchemaGenerator(MoreMappers.initMapper(), config)

    val mapper: ObjectMapper = MoreMappers.initMapper().apply {
        setSerializationInclusion(JsonInclude.Include.NON_NULL)
        registerModule(KotlinModule.Builder().build())
    }

    private val jsonSchemaFactory: JsonSchemaFactory =
        JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7)
}
