package io.airbyte.integrations.source.oracle

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.cdk.core.command.CONNECTOR_CONFIG_PREFIX
import io.airbyte.cdk.core.command.ConnectorConfigurationSupplier
import io.airbyte.cdk.core.command.SourceConnectorConfiguration
import io.airbyte.cdk.core.command.SshKeyAuthTunnelConfiguration
import io.airbyte.cdk.core.command.SshNoTunnelConfiguration
import io.airbyte.cdk.core.command.SshPasswordAuthTunnelConfiguration
import io.airbyte.cdk.core.command.SshTunnelConfiguration
import io.airbyte.cdk.core.command.SshTunnelConfigurationPOJO
import io.airbyte.commons.exceptions.ConfigErrorException
import io.airbyte.commons.exceptions.ConnectionErrorException
import io.airbyte.commons.io.IOs
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.resources.MoreResources
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.validation.json.JsonSchemaValidator
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.ConfigurationBuilder
import io.micronaut.context.annotation.ConfigurationProperties
import java.io.File
import java.io.FileOutputStream
import java.io.StringReader
import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.security.KeyStore
import java.security.cert.Certificate
import java.security.cert.CertificateFactory
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.util.*
import java.util.function.Consumer
import oracle.jdbc.OracleDatabaseMetaData
import org.apache.commons.lang3.RandomStringUtils
import org.bouncycastle.util.io.pem.PemReader

private val logger = KotlinLogging.logger {}

data class OracleSourceConfiguration (
        override val realHost: String,
        override val realPort: Int,
        override val sshTunnel: SshTunnelConfiguration,
        override val jdbcUrl: String,
        override val jdbcProperties: Map<String, String>,
        val defaultSchema: String,
        override val schemas: List<String>
) : SourceConnectorConfiguration {
    override fun getDefaultNamespace(): Optional<String> = Optional.of(defaultSchema)

    override val expectedStateType = AirbyteStateMessage.AirbyteStateType.STREAM

}

@ConfigurationProperties(CONNECTOR_CONFIG_PREFIX)
private class ConfigurationPOJO : ConnectorConfigurationSupplier<OracleSourceConfiguration> {

    @JsonIgnore
    var json: String? = null

    private val validated: OracleSourceConfiguration by lazy {
        buildConfiguration(json ?: Jsons.serialize(this))
    }

    override fun get(): OracleSourceConfiguration = validated

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
    @ConfigurationBuilder(configurationPrefix = "connection_data")
    val connectionData = ConnectionDataPOJO()

    @JsonProperty("encryption")
    @ConfigurationBuilder(configurationPrefix = "encryption")
    val encryption = EncryptionPOJO()

    @JsonProperty("tunnel_method")
    @ConfigurationBuilder(configurationPrefix = "tunnel_method")
    var tunnelMethod = SshTunnelConfigurationPOJO()
}

@ConfigurationProperties("$CONNECTOR_CONFIG_PREFIX.connection_data")
private class ConnectionDataPOJO {

    @JsonProperty("connection_type")
    var connectionType: String? = null

    @JsonProperty("service_name")
    var serviceName: String? = null

    @JsonProperty("sid")
    var sid: String? = null
}

@ConfigurationProperties("$CONNECTOR_CONFIG_PREFIX.encryption")
private class EncryptionPOJO {

    @JsonProperty("encryption_method")
    var encryptionMethod: String = "unencrypted"

    @JsonProperty("encryption_algorithm")
    var encryptionAlgorithm: String? = null

    @JsonProperty("ssl_certificate")
    var sslCertificate: String? = null
}

private fun buildConfiguration(jsonString: String): OracleSourceConfiguration {
    val json: JsonNode = Jsons.deserialize(jsonString)
    val schema = Jsons.deserialize(MoreResources.readBytes("spec.json"))
    val results = JsonSchemaValidator().validate(schema, json)
    if (results.isNotEmpty()) {
        throw ConfigErrorException("config json schema violation: ${results.first()}")
    }
    val pojo = Jsons.`object`(json, ConfigurationPOJO::class.java)
    val realHost: String = pojo.host!!
    val realPort: Int = pojo.port!!
    val sshTunnel: SshTunnelConfiguration = pojo.tunnelMethod.get()
    val (host, port) = when (sshTunnel) {
        is SshKeyAuthTunnelConfiguration -> sshTunnel.host to sshTunnel.port
        is SshPasswordAuthTunnelConfiguration -> sshTunnel.host to sshTunnel.port
        is SshNoTunnelConfiguration -> realHost to realPort
    }
    val jdbcProperties = mutableMapOf<String, String>()
    jdbcProperties["user"] = pojo.username!!
    pojo.password?.let { jdbcProperties["password"] = it }
    /*
        * The property useFetchSizeWithLongColumn required to select LONG or LONG RAW columns. Oracle
        * recommends avoiding LONG and LONG RAW columns. Use LOB instead. They are included in Oracle
        * only for legacy reasons.
        *
        * THIS IS A THIN ONLY PROPERTY. IT SHOULD NOT BE USED WITH ANY OTHER DRIVERS.
        *
        * See https://docs.oracle.com/cd/E11882_01/appdev.112/e13995/oracle/jdbc/OracleDriver.html
        * https://docs.oracle.com/cd/B19306_01/java.102/b14355/jstreams.htm#i1014085
        */
    jdbcProperties["oracle.jdbc.useFetchSizeWithLongColumn"] = "true"
    // Parse URL parameters.
    val pattern = "^([^=]+)=(.*)$".toRegex()
    for (pair in (pojo.jdbcUrlParams ?: "").trim().split("&".toRegex())) {
        if (pair.isBlank()) {
            continue
        }
        val result: MatchResult? = pattern.matchEntire(pair)
        if (result == null) {
            logger.warn { "ignoring invalid JDBC URL param '$pair'" }
        } else {
            val key: String = result.groupValues[1].trim()
            val urlEncodedValue: String = result.groupValues[2].trim()
            jdbcProperties[key] = URLDecoder.decode(urlEncodedValue, StandardCharsets.UTF_8)
        }
    }
    // Determine protocol.
    val protocol: String = when (pojo.encryption.encryptionMethod) {
        "unencrypted", "client_nne" -> "TCP"
        "encrypted_verify_certificate" -> "TCPS"
        else -> throw ConfigErrorException(
            "invalid encryption method ${pojo.encryption.encryptionMethod}",
        )
    }
    if (protocol == "TCPS") {
        val keyStoreFile = File(IOs.writeFileToRandomTmpDir("clientkeystore.jks", ""))
        keyStoreFile.deleteOnExit()
        val certPemContents: String = pojo.encryption.sslCertificate!!
        val pemReader = PemReader(StringReader(certPemContents))
        val certDer = pemReader.readPemObject().content
        val cf: CertificateFactory = CertificateFactory.getInstance("X.509")
        val cert: Certificate = cf.generateCertificate(certDer.inputStream())
        val keyStore = KeyStore.getInstance(KeyStore.getDefaultType())
        keyStore.load(null, null) // Initialize the KeyStore
        keyStore.setCertificateEntry("rds-root", cert)
        val keyStorePass: String = RandomStringUtils.randomAlphanumeric(8)
        val fos = FileOutputStream(keyStoreFile)
        keyStore.store(fos, keyStorePass.toCharArray())
        fos.close()
        jdbcProperties["javax.net.ssl.trustStore"] = keyStoreFile.toString()
        jdbcProperties["javax.net.ssl.trustStoreType"] = "JKS"
        jdbcProperties["javax.net.ssl.trustStorePassword"] = keyStorePass
    } else if (pojo.encryption.encryptionMethod == "client_nne") {
        val algorithm: String = pojo.encryption.encryptionAlgorithm!!
        jdbcProperties["oracle.net.encryption_client"] = "REQUIRED"
        jdbcProperties["oracle.net.encryption_types_client"] = "( $algorithm )"
    }
    // Build JDBC URL
    val address = "(ADDRESS=(PROTOCOL=${protocol})(HOST=${host})(PORT=${port}))"
    val connectData: String = when (pojo.connectionData.connectionType) {
        null -> ""
        "service_name" -> "(CONNECT_DATA=(SERVICE_NAME=${pojo.connectionData.serviceName!!}))"
        "sid" -> "(CONNECT_DATA=(SID=${pojo.connectionData.sid!!}))"
        else -> throw ConfigErrorException("config does not specify a valid connection_type")
    }
    val jdbcUrl = "jdbc:oracle:thin:@(DESCRIPTION=${address}${connectData})"
    val defaultSchema: String = pojo.username!!.uppercase()
    return OracleSourceConfiguration(
        realHost = realHost,
        realPort = realPort,
        sshTunnel = sshTunnel,
        jdbcUrl = jdbcUrl,
        jdbcProperties = jdbcProperties,
        defaultSchema = defaultSchema,
        schemas = pojo.schemas.ifEmpty { listOf(defaultSchema) },
    )
}
