package io.airbyte.integrations.source.oracle

import io.airbyte.cdk.core.context.env.ConnectorConfigurationPropertySource
import io.airbyte.integrations.source.oracle.operation.executor.OracleSourceCheckOperationExecutor
import io.micronaut.context.annotation.Property
import io.micronaut.context.env.Environment
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Test

@MicronautTest(environments = [Environment.TEST, "source"])
@Property(name = ConnectorConfigurationPropertySource.CONNECTOR_OPERATION, value = "check")
class OracleSourceCheckTest {

    @Inject
    lateinit var checkOperationExecutor: OracleSourceCheckOperationExecutor

    @Test
    @Property(name = "airbyte.connector.config.host", value = "localhost")
    @Property(name = "airbyte.connector.config.port", value = "12345")
    @Property(name = "airbyte.connector.config.username", value = "bob")
    @Property(name = "airbyte.connector.config.schemas", value = "foo,bar")
    @Property(name = "airbyte.connector.config.connection_data.connection_type", value = "service_name")
    @Property(name = "airbyte.connector.config.connection_data.service_name", value = "myname")
    internal fun testConfig() {
        checkOperationExecutor.execute()

    }


}
