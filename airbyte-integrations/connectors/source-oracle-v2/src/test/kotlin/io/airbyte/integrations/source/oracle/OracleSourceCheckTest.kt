/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.oracle

import io.airbyte.cdk.operation.CONNECTOR_OPERATION
import io.airbyte.cdk.operation.DefaultCheckOperation
import io.airbyte.commons.io.IOs
import io.micronaut.context.annotation.Property
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import java.io.File
import org.junit.jupiter.api.Test

@MicronautTest(environments = ["source"])
@Property(name = CONNECTOR_OPERATION, value = "check")
class OracleSourceCheckTest {

    @Inject lateinit var checkOperation: DefaultCheckOperation

    @Test
    @Property(name = "airbyte.connector.config.host", value = "localhost")
    @Property(name = "airbyte.connector.config.port", value = "1521")
    @Property(name = "airbyte.connector.config.username", value = "FOO")
    @Property(name = "airbyte.connector.config.password", value = "BAR")
    @Property(
        name = "airbyte.connector.config.connection_data.connection_type",
        value = "service_name"
    )
    @Property(name = "airbyte.connector.config.connection_data.service_name", value = "FREEPDB1")
    @Property(name = "airbyte.connector.config.tunnel_method.tunnel_method", value = "NO_TUNNEL")
    internal fun testConfig() {
        checkOperation.execute()
    }

    @Test
    internal fun testConfigCli() {
        val sslCert =
            """
            -----BEGIN CERTIFICATE-----
            MIIDizCCAnOgAwIBAgIUVWCfGs+uSa8Kcuzj3d/IkYbYMCwwDQYJKoZIhvcNAQEL
            BQAwVTELMAkGA1UEBhMCQ0ExCzAJBgNVBAgMAlFDMRYwFAYDVQQHDA1EcnVtbW9u
            ZHZpbGxlMSEwHwYDVQQKDBhJbnRlcm5ldCBXaWRnaXRzIFB0eSBMdGQwHhcNMjQw
            NDA1MDMxNDA0WhcNMjUwNDA1MDMxNDA0WjBVMQswCQYDVQQGEwJDQTELMAkGA1UE
            CAwCUUMxFjAUBgNVBAcMDURydW1tb25kdmlsbGUxITAfBgNVBAoMGEludGVybmV0
            IFdpZGdpdHMgUHR5IEx0ZDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEB
            AOW7ZXDcu27bT2HfyTkLZ1lwNKwVYLHirBorpdMWqlZucBqpflh9snijmapgEhkY
            EXdWtNW2kp5isrRB/AgwwqepbPFrsZGM7U9XDMzmRDENFF3+R3zYouyEONAzVl+P
            SJYmeRm6xIbz1+L/YXrtc4clRoQN9J1opmqMzeMi74ShHoBFVHyuJr1QZFC2otij
            Gw9IaJ3IWNThaXm+Txits5cyMkAKbUSkNJs4tjtbPpkOJsvhvZiWvQFHtaH+Cm9M
            i4bwnZlKCN/1Ubn40of/nsEsIQlzIfY90ydswPy68azxFDNFE22SxNw+gPF3sJ99
            Y9T61IqNV1VLhQfEheo2mHUCAwEAAaNTMFEwHQYDVR0OBBYEFCmUa/lmXwJxFN5A
            sRlVfzlcrs/uMB8GA1UdIwQYMBaAFCmUa/lmXwJxFN5AsRlVfzlcrs/uMA8GA1Ud
            EwEB/wQFMAMBAf8wDQYJKoZIhvcNAQELBQADggEBAFVnpF91oNhkIMg3cIJCFCIi
            WCtPdG9njY9S5zcH7S8ZsQyRiODRL7OEkqhT3frGWgjFiVHRNatuOyyra8KracpD
            hjyRWw/FMTT2+2zhf7cKqdB5kwAiDTr/CGcV8pYqy1YVjrHJ0SbkD+1i2AXJg6ks
            2NfdHiWiAG56+xygyu5k5kUpF2KAVQLK7oaIPsazP1aGiAckYKDNzt2to3dNq9B8
            DCDug44eK1q3ciBAojpbVjJO/sRLyXl5EGsJv2fAOFCC9NrcBhWqFulktQVZu7Wd
            kRj5zGaGRdL+l0Io1vHgJY7jNf3qp9F0uo4MIumj4CXRxq115zCbeznr0wr5/j0=
            -----END CERTIFICATE-----
        """
                .trimIndent()
                .trim()
                .replace("\n", "\\n")
        val configFile =
            File(
                IOs.writeFileToRandomTmpDir(
                    "config.json",
                    """
            {
                "host": "localhost",
                "port": 12345,
                "username": "bob",
                "schemas": ["foo", "bar"],
                "encryption": {"encryption_method": "encrypted_verify_certificate", "ssl_certificate": "$sslCert"},
                "connection_data": {"connection_type": "service_name", "service_name": "myname"},
                "tunnel_method": {"tunnel_method": "FOO"}
            }
        """.trimIndent()
                )
            )
        configFile.deleteOnExit()
        OracleSource.main(
            arrayOf("--check", "--config", configFile.toPath().toAbsolutePath().toString())
        )
    }

    @Test
    internal fun testCatalogCli() {
        val configFile =
            File(
                IOs.writeFileToRandomTmpDir(
                    "config.json",
                    """
            {
                "host": "localhost",
                "port": 12345,
                "username": "bob",
                "schemas": ["foo", "bar"],
                "connection_data": {"connection_type": "service_name", "service_name": "myname"}
            }
        """.trimIndent()
                )
            )
        configFile.deleteOnExit()

        val streamName = "test-name"
        val streamNamespace = "test-namespace"
        val catalogJson =
            "{\"streams\":[{\"stream\":{\"name\":\"$streamName\",\"namespace\":\"$streamNamespace\"}}]}"
        val catalogFile = File(IOs.writeFileToRandomTmpDir("catalog.json", catalogJson))
        catalogFile.deleteOnExit()

        OracleSource.main(
            arrayOf(
                "--read",
                "--config",
                configFile.toString(),
                "--catalog",
                catalogFile.toString()
            )
        )
    }
}
