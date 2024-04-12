/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.oracle

import io.airbyte.cdk.command.ConnectorConfigurationJsonObjectWrapper
import io.micronaut.context.annotation.Property
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Test

@MicronautTest(environments = ["source"])
class OracleSourceConfigurationTest {

    @Inject lateinit var config: ConnectorConfigurationJsonObjectWrapper<OracleSourceConfigurationJsonObject>

    @Test
    @Property(name = "airbyte.connector.config.host", value = "localhost")
    @Property(name = "airbyte.connector.config.port", value = "1521")
    @Property(name = "airbyte.connector.config.username", value = "FOO")
    @Property(name = "airbyte.connector.config.password", value = "BAR")
    @Property(name = "airbyte.connector.config.schemas", value = "FOO")
    @Property(
        name = "airbyte.connector.config.connection_data.connection_type",
        value = "service_name"
    )
    @Property(name = "airbyte.connector.config.connection_data.service_name", value = "FREEPDB1")
    /*
    @Property(name = "airbyte.connector.config.tunnel_method.tunnel_method", value = "SSH_PASSWORD_AUTH")
    @Property(name = "airbyte.connector.config.tunnel_method.tunnel_host", value = "localhost")
    @Property(name = "airbyte.connector.config.tunnel_method.tunnel_port", value = "2222")
    @Property(name = "airbyte.connector.config.tunnel_method.tunnel_user", value = "sshuser")
    @Property(name = "airbyte.connector.config.tunnel_method.tunnel_user_password", value = "secret")

     */
    internal fun testConfig() {
        config.get()
    }

}
