package io.airbyte.cdk.core.command.option

import io.airbyte.validation.json.JsonSchemaValidator
import io.micronaut.context.annotation.Factory
import jakarta.inject.Singleton

@Factory
class ValidationFactory {

    @Singleton
    fun jsonSchemaValidator(): JsonSchemaValidator {
        return JsonSchemaValidator()
    }
}
