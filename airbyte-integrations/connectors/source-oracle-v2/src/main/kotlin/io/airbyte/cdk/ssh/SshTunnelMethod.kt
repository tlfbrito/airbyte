/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.ssh

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonPropertyDescription
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaDefault
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaDescription
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import io.airbyte.cdk.command.CONNECTOR_CONFIG_PREFIX
import io.airbyte.commons.exceptions.ConfigErrorException
import io.micronaut.context.annotation.ConfigurationProperties
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "tunnel_method")
@JsonSubTypes(
    JsonSubTypes.Type(value = SshNoTunnelMethod::class, name = "NO_TUNNEL"),
    JsonSubTypes.Type(value = SshKeyAuthTunnelMethod::class, name = "SSH_KEY_AUTH"),
    JsonSubTypes.Type(value = SshPasswordAuthTunnelMethod::class, name = "SSH_PASSWORD_AUTH"),
)
@JsonSchemaTitle("SSH Tunnel Method")
@JsonSchemaDescription(
    "Whether to initiate an SSH tunnel before connecting to the database, and if so, which kind of authentication to use."
)
sealed interface SshTunnelMethod

sealed interface SshTunnelMethodSubType : SshTunnelMethod

@JsonSchemaTitle("No Tunnel")
@JsonSchemaDescription("No ssh tunnel needed to connect to database")
data object SshNoTunnelMethod : SshTunnelMethodSubType

@JsonSchemaTitle("SSH Key Authentication")
@JsonSchemaDescription("Connect through a jump server tunnel host using username and ssh key")
data class SshKeyAuthTunnelMethod(
    @JsonProperty("tunnel_host", required = true)
    @JsonSchemaTitle("SSH Tunnel Jump Server Host")
    @JsonPropertyDescription("Hostname of the jump server host that allows inbound ssh tunnel.")
    @JsonSchemaInject(json = """{"order":1}""")
    val host: String,
    @JsonProperty("tunnel_port", required = true)
    @JsonSchemaTitle("SSH Connection Port")
    @JsonPropertyDescription("Port on the proxy/jump server that accepts inbound ssh connections.")
    @JsonSchemaInject(json = """{"order":2,"minimum": 0,"maximum": 65536}""")
    @JsonSchemaDefault("22")
    val port: Int,
    @JsonProperty("tunnel_user", required = true)
    @JsonSchemaTitle("SSH Login Username")
    @JsonPropertyDescription("OS-level username for logging into the jump server host")
    @JsonSchemaInject(json = """{"order":3}""")
    val user: String,
    @JsonProperty("ssh_key", required = true)
    @JsonSchemaTitle("SSH Private Key")
    @JsonPropertyDescription(
        "OS-level user account ssh key credentials in RSA PEM format " +
            "( created with ssh-keygen -t rsa -m PEM -f myuser_rsa )"
    )
    @JsonSchemaInject(json = """{"order":4,"multiline":true,"airbyte_secret": true}""")
    val key: String,
) : SshTunnelMethodSubType

@JsonSchemaTitle("Password Authentication")
@JsonSchemaDescription(
    "Connect through a jump server tunnel host using username and password authentication"
)
data class SshPasswordAuthTunnelMethod(
    @JsonProperty("tunnel_host", required = true)
    @JsonSchemaTitle("SSH Tunnel Jump Server Host")
    @JsonPropertyDescription("Hostname of the jump server host that allows inbound ssh tunnel.")
    @JsonSchemaInject(json = """{"order":1}""")
    val host: String,
    @JsonProperty("tunnel_port", required = true)
    @JsonSchemaTitle("SSH Connection Port")
    @JsonPropertyDescription("Port on the proxy/jump server that accepts inbound ssh connections.")
    @JsonSchemaInject(json = """{"order":2,"minimum": 0,"maximum": 65536}""")
    @JsonSchemaDefault("22")
    val port: Int,
    @JsonProperty("tunnel_user", required = true)
    @JsonSchemaTitle("SSH Login Username")
    @JsonPropertyDescription("OS-level username for logging into the jump server host")
    @JsonSchemaInject(json = """{"order":3}""")
    val user: String,
    @JsonProperty("tunnel_user_password", required = true)
    @JsonSchemaTitle("Password")
    @JsonPropertyDescription("OS-level password for logging into the jump server host")
    @JsonSchemaInject(json = """{"order":4,"airbyte_secret": true}""")
    val password: String,
) : SshTunnelMethodSubType

@ConfigurationProperties("$CONNECTOR_CONFIG_PREFIX.tunnel_method")
class MicronautFriendlySshTunnelMethod : SshTunnelMethod {

    var tunnelMethod: String = "NO_TUNNEL"
    var tunnelHost: String? = null
    var tunnelPort: Int = 22
    var tunnelUser: String? = null
    var sshKey: String? = null
    var tunnelUserPassword: String? = null

    fun asSshTunnelMethodSubType(): SshTunnelMethodSubType =
        when (tunnelMethod) {
            "NO_TUNNEL" -> SshNoTunnelMethod
            "SSH_KEY_AUTH" ->
                SshKeyAuthTunnelMethod(tunnelHost!!, tunnelPort, tunnelUser!!, sshKey!!)
            "SSH_PASSWORD_AUTH" ->
                SshPasswordAuthTunnelMethod(
                    tunnelHost!!,
                    tunnelPort,
                    tunnelUser!!,
                    tunnelUserPassword!!
                )
            else -> throw ConfigErrorException("invalid value $tunnelMethod")
        }
}
