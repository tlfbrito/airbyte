package io.airbyte.integrations.source.oracle;

import io.airbyte.cdk.core.AirbyteConnectorRunner;
import io.airbyte.cdk.core.IntegrationCommand;

public class OracleSource {

  static public void main(String[] args) {
    AirbyteConnectorRunner.run(AirbyteConnectorRunner.ConnectorType.SOURCE, IntegrationCommand.class, args);
  }
}
