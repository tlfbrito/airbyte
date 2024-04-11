package io.airbyte.cdk.jdbc

import io.airbyte.cdk.command.ConnectorConfigurationSupplier
import io.airbyte.cdk.command.SourceConnectorConfiguration
import io.airbyte.cdk.ssh.TunnelSession
import io.airbyte.cdk.ssh.createTunnelSession
import io.airbyte.commons.exceptions.ConfigErrorException
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.io.StringReader
import java.net.InetSocketAddress
import java.security.Security
import java.sql.Connection
import java.sql.DriverManager
import java.time.Duration
import java.util.*
import java.util.function.Supplier
import kotlin.time.toJavaDuration
import org.apache.sshd.client.SshClient
import org.apache.sshd.client.keyverifier.AcceptAllServerKeyVerifier
import org.apache.sshd.client.session.ClientSession
import org.apache.sshd.common.SshException
import org.apache.sshd.common.session.SessionHeartbeatController
import org.apache.sshd.common.util.net.SshdSocketAddress
import org.apache.sshd.common.util.security.SecurityUtils
import org.apache.sshd.core.CoreModuleProperties
import org.apache.sshd.server.forward.AcceptAllForwardingFilter
import org.bouncycastle.jce.provider.BouncyCastleProvider

private val logger = KotlinLogging.logger {}

@Singleton
class JdbcConnectionFactory(
    private val configSupplier: ConnectorConfigurationSupplier<SourceConnectorConfiguration>
) : Supplier<Connection>, AutoCloseable {

    private val config: SourceConnectorConfiguration by lazy { configSupplier.get() }

    private val tunnelSessionDelegate: Lazy<TunnelSession> = lazy {
        val remote = SshdSocketAddress(config.realHost.trim(), config.realPort)
        createTunnelSession(remote, config.sshTunnel, config.sshConnectionOptions)
    }

    override fun close() {
        if (tunnelSessionDelegate.isInitialized()) {
            tunnelSessionDelegate.value.close()
        }
    }

    override fun get(): Connection {
        val address: InetSocketAddress = tunnelSessionDelegate.value.address
        val jdbcUrl: String = String.format(config.jdbcUrlFmt, address.hostName, address.port)
        logger.info { "Creating new connection for '$jdbcUrl'." }
        val props = Properties().apply { putAll(config.jdbcProperties) }
        return DriverManager.getConnection(jdbcUrl, props).also { it.isReadOnly = true }
    }
}
