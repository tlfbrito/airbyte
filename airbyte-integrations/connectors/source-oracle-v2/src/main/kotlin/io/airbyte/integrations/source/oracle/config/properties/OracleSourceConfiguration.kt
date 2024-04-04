package io.airbyte.integrations.source.oracle.config.properties

import com.fasterxml.jackson.annotation.JsonProperty
import io.airbyte.cdk.core.command.option.ConnectorConfiguration
import io.micronaut.context.annotation.ConfigurationProperties
import java.util.*

@ConfigurationProperties("airbyte.connector.config")
class OracleSourceConfiguration : ConnectorConfiguration {

    @JsonProperty("host", required = true)
    var host: String? = null

    @JsonProperty("port", required = true)
    var port: Int? = null

    @JsonProperty("username", required = true)
    var username: String? = null

    @JsonProperty("password")
    var password: String? = null

    @JsonProperty("schemas")
    var schemas: List<String> = listOf()

    @JsonProperty("jdbc_url_params")
    var jdbcUrlParams: String? = null

    @JsonProperty("connection_data")
    var connectionData: OracleSourceConnectionDataConfiguration? = null

    override fun getDefaultNamespace(): Optional<String> {
        return Optional.ofNullable(username);
    }

    override fun getRawNamespace(): Optional<String> {
        return Optional.empty()
    }

}

class OracleSourceConnectionDataConfiguration(connectionData: Map<String, Any?>) {

    @JsonProperty("connection_type")
    val connectionType: String = connectionData.getOrDefault("connection_type", "").toString()

    @JsonProperty("service_name")
    val serviceName: String? = connectionData["service_name"] as String?

    @JsonProperty("sid")
    val sid: String? = connectionData["sid"] as String?
}

class OracleSourceEncryptionConfiguration {

    @JsonProperty("encryption_method")
    var encryptionMethod: String? = null

    @JsonProperty("encryption_algorithm")
    var encryptionAlgorithm: String? = null

    @JsonProperty("ssl_certificate")
    var sslCertificate: String? = null
}
