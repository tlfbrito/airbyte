package io.airbyte.cdk.consumers

import io.airbyte.commons.json.Jsons
import io.airbyte.protocol.models.v0.AirbyteMessage
import jakarta.inject.Singleton
import java.util.function.Consumer

fun interface OutputConsumer : Consumer<AirbyteMessage>

@Singleton
class DefaultOutputConsumer : Consumer<AirbyteMessage> {
    override fun accept(airbyteMessage: AirbyteMessage) {
        val json: String = Jsons.serialize(airbyteMessage)
        synchronized(this) {
            println(json)
        }
    }
}
