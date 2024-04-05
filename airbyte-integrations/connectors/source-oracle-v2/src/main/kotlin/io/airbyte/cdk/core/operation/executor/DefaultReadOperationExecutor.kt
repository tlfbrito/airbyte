/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.core.operation.executor

import io.airbyte.cdk.core.context.env.ConnectorConfigurationPropertySource
import io.airbyte.cdk.core.operation.OperationExecutionException
import io.airbyte.cdk.core.util.ShutdownUtils
import io.airbyte.commons.util.AutoCloseableIterator
import io.airbyte.protocol.models.AirbyteStreamNameNamespacePair
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Bean
import io.micronaut.context.annotation.ConfigurationProperties
import io.micronaut.context.annotation.Requires
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.Optional
import java.util.concurrent.TimeUnit
import java.util.function.Consumer

private val logger = KotlinLogging.logger {}

@Singleton
@Named("readOperationExecutor")
@Requires(
    property = ConnectorConfigurationPropertySource.CONNECTOR_OPERATION,
    value = "read",
)
//@Requires(env = ["source"])
class DefaultReadOperationExecutor(
    private val messageIterator: Optional<AutoCloseableIterator<AirbyteMessage>>,
    @Named("outputRecordCollector") private val outputRecordCollector: Consumer<AirbyteMessage>,
    private val shutdownUtils: ShutdownUtils,
) : OperationExecutor {

    @Bean
    @ConfigurationProperties("airbyte.connector.catalog.json")
    fun configuredCatalog(): ConfiguredAirbyteCatalog = ConfiguredAirbyteCatalog()

    override fun execute(): Result<Sequence<AirbyteMessage>> {
        val catalog = configuredCatalog()
        logger.info { "$catalog" }
        return Result.success(sequenceOf())
    }
}
