package io.airbyte.cdk.ssh

import com.fasterxml.jackson.annotation.JsonProperty
import io.airbyte.cdk.command.CONNECTOR_CONFIG_PREFIX
import io.micronaut.context.annotation.ConfigurationProperties
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

@ConfigurationProperties("$CONNECTOR_CONFIG_PREFIX.ssh_connection_options")
class SshConnectionOptionsPOJO {

    @JsonProperty("session_heartbeat_interval")
    var sessionHeartbeatIntervalMillis: Long = 1_000

    @JsonProperty("global_heartbeat_interval")
    var globalHeartbeatIntervalMillis: Long = 2_000

    @JsonProperty("idle_timeout")
    var idleTimeoutMillis: Long = 0 // infinite

}

data class SshConnectionOptions(
    val sessionHeartbeatInterval: Duration,
    val globalHeartbeatInterval: Duration,
    val idleTimeout: Duration,
) {
    companion object {
        @JvmStatic fun from(pojo: SshConnectionOptionsPOJO) = SshConnectionOptions(
            pojo.sessionHeartbeatIntervalMillis.milliseconds,
            pojo.globalHeartbeatIntervalMillis.milliseconds,
            pojo.idleTimeoutMillis.milliseconds
        )
    }
}
