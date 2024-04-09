package io.airbyte.integrations.source.oracle

import io.airbyte.cdk.core.operation.CONNECTOR_OPERATION
import io.airbyte.cdk.core.operation.DefaultDiscoverOperation
import io.micronaut.context.annotation.Property
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Test

@MicronautTest(environments = ["source"])
@Property(name = CONNECTOR_OPERATION, value = "discover")
class OracleSourceDiscoverTest {

    @Inject
    lateinit var discoverOperation: DefaultDiscoverOperation

    @Test
    @Property(name = "airbyte.connector.config.host", value = "localhost")
    @Property(name = "airbyte.connector.config.port", value = "1521")
    @Property(name = "airbyte.connector.config.username", value = "FOO")
    @Property(name = "airbyte.connector.config.password", value = "BAR")
    @Property(name = "airbyte.connector.config.connection_data.connection_type", value = "service_name")
    @Property(name = "airbyte.connector.config.connection_data.service_name", value = "FREEPDB1")
    @Property(name = "airbyte.connector.config.tunnel_method.tunnel_method", value = "NO_TUNNEL")
    internal fun testDiscover() {
        discoverOperation.execute()
    }

}

