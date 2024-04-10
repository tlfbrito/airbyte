package io.airbyte.integrations.source.oracle

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import ddtrot.com.squareup.moshi.Json
import io.airbyte.cdk.consumers.DefaultOutputRecordCollector
import io.airbyte.cdk.operation.CONNECTOR_OPERATION
import io.airbyte.cdk.operation.DefaultCheckOperation
import io.airbyte.cdk.operation.DefaultSpecOperation
import io.airbyte.commons.io.IOs
import io.airbyte.commons.jackson.MoreMappers
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.resources.MoreResources
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.json.compare.JSONCompare
import io.micronaut.context.annotation.Property
import io.micronaut.test.annotation.MockBean
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import jakarta.inject.Named
import java.io.StringWriter
import java.util.function.Consumer
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

@MicronautTest(environments = ["source"])
@Property(name = CONNECTOR_OPERATION, value = "spec")
class OracleSourceSpecTest {

    @Inject
    lateinit var specOperation: DefaultSpecOperation

    private var latestSpec = ConnectorSpecification()

    @MockBean(DefaultOutputRecordCollector::class)
    @Named("outputRecordCollector")
    fun outputRecordCollector(): Consumer<AirbyteMessage> = Consumer<AirbyteMessage> {
        if (it.type == AirbyteMessage.Type.SPEC) {
            synchronized(latestSpec) {
                latestSpec = it.spec
            }
        }
    }

    @Test
    internal fun testSpec() {
        val expected: ConnectorSpecification = Jsons.deserialize(
            MoreResources.readResourceAsFile("spec.json"),
            ConnectorSpecification::class.java)
        specOperation.execute()
        val actual: ConnectorSpecification = latestSpec
        println(Jsons.serialize(actual))
        JSONCompare.assertMatches(expected, actual)
    }

}

