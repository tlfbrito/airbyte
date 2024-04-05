package io.airbyte.cdk.core.command.option

import com.fasterxml.jackson.annotation.JsonProperty
import io.airbyte.cdk.integrations.base.ssh.SshTunnel
import io.micronaut.context.annotation.ConfigurationProperties
import java.util.function.Supplier

sealed interface SshTunnelConfiguration {
    fun tunnelMethod(): SshTunnel.TunnelMethod
}

data object SshNoTunnelConfiguration : SshTunnelConfiguration {
    override fun tunnelMethod() = SshTunnel.TunnelMethod.NO_TUNNEL

}

data class SshKeyAuthTunnelConfiguration(
    val host: String,
    val port: Int,
    val user: String,
    val key: String,
) : SshTunnelConfiguration {
    override fun tunnelMethod() = SshTunnel.TunnelMethod.SSH_KEY_AUTH

}

data class SshPasswordAuthTunnelConfiguration(
    val host: String,
    val port: Int,
    val user: String,
    val password: String,
) : SshTunnelConfiguration {
    override fun tunnelMethod() = SshTunnel.TunnelMethod.SSH_PASSWORD_AUTH

}

interface SshTunnelConfigurationSupplier : Supplier<SshTunnelConfiguration>

@ConfigurationProperties("$CONNECTOR_CONFIG_PREFIX.tunnel_method")
class SshTunnelConfigurationPOJO : SshTunnelConfigurationSupplier {

    private val validated: SshTunnelConfiguration by lazy {
        when (SshTunnel.TunnelMethod.valueOf(tunnelMethod.uppercase())) {
            SshTunnel.TunnelMethod.NO_TUNNEL -> SshNoTunnelConfiguration
            SshTunnel.TunnelMethod.SSH_KEY_AUTH -> SshKeyAuthTunnelConfiguration(tunnelHost!!, tunnelPort, tunnelUser!!, sshKey!!)
            SshTunnel.TunnelMethod.SSH_PASSWORD_AUTH -> SshPasswordAuthTunnelConfiguration(tunnelHost!!, tunnelPort, tunnelUser!!, tunnelUserPassword!!)
        }
    }

    override fun get(): SshTunnelConfiguration = validated

    @JsonProperty("tunnel_method")
    var tunnelMethod: String = "NO_TUNNEL"

    @JsonProperty("tunnel_host")
    var tunnelHost: String? = null

    @JsonProperty("tunnel_port")
    var tunnelPort: Int = 22

    @JsonProperty("tunnel_user")
    var tunnelUser: String? = null

    @JsonProperty("ssh_key")
    var sshKey: String? = null

    @JsonProperty("tunnel_user_password")
    var tunnelUserPassword: String? = null
}
