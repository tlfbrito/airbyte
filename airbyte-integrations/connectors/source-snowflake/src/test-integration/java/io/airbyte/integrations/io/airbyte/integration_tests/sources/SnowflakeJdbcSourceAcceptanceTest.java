/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.io.airbyte.integration_tests.sources;

import static io.airbyte.integrations.base.errors.utils.ConnectionErrorType.INCORRECT_USERNAME_OR_HOST;
import static io.airbyte.integrations.base.errors.utils.ConnectionErrorType.INCORRECT_USERNAME_OR_PASSWORD;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableSet;
import io.airbyte.commons.io.IOs;
import io.airbyte.commons.json.Jsons;
import io.airbyte.db.factory.DataSourceFactory;
import io.airbyte.integrations.source.jdbc.AbstractJdbcSource;
import io.airbyte.integrations.source.jdbc.test.JdbcSourceAcceptanceTest;
import io.airbyte.integrations.source.snowflake.SnowflakeSource;
import io.airbyte.protocol.models.AirbyteConnectionStatus;
import io.airbyte.protocol.models.AirbyteConnectionStatus.Status;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.JDBCType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SnowflakeJdbcSourceAcceptanceTest extends JdbcSourceAcceptanceTest {

  private static JsonNode snConfig;

  @BeforeAll
  static void init() {
    snConfig = Jsons
        .deserialize(IOs.readFile(Path.of("secrets/config.json")));
    // due to case sensitiveness in SnowflakeDB
    SCHEMA_NAME = "JDBC_INTEGRATION_TEST1";
    SCHEMA_NAME2 = "JDBC_INTEGRATION_TEST2";
    TEST_SCHEMAS = ImmutableSet.of(SCHEMA_NAME, SCHEMA_NAME2);
    TABLE_NAME = "ID_AND_NAME";
    TABLE_NAME_WITH_SPACES = "ID AND NAME";
    TABLE_NAME_WITHOUT_PK = "ID_AND_NAME_WITHOUT_PK";
    TABLE_NAME_COMPOSITE_PK = "FULL_NAME_COMPOSITE_PK";
    COL_ID = "ID";
    COL_NAME = "NAME";
    COL_UPDATED_AT = "UPDATED_AT";
    COL_FIRST_NAME = "FIRST_NAME";
    COL_LAST_NAME = "LAST_NAME";
    COL_LAST_NAME_WITH_SPACE = "LAST NAME";
    ID_VALUE_1 = new BigDecimal(1);
    ID_VALUE_2 = new BigDecimal(2);
    ID_VALUE_3 = new BigDecimal(3);
    ID_VALUE_4 = new BigDecimal(4);
    ID_VALUE_5 = new BigDecimal(5);
  }

  @BeforeEach
  public void setup() throws Exception {
    super.setup();
  }

  @AfterEach
  public void clean() throws Exception {
    super.tearDown();
    DataSourceFactory.close(dataSource);
  }

  @Override
  public boolean supportsSchemas() {
    return true;
  }

  @Override
  public JsonNode getConfig() {
    return Jsons.clone(snConfig);
  }

  @Override
  public String getDriverClass() {
    return SnowflakeSource.DRIVER_CLASS;
  }

  @Override
  public AbstractJdbcSource<JDBCType> getJdbcSource() {
    return new SnowflakeSource();
  }

  @Test
  void testCheckIncorrectPasswordFailure() throws Exception {
    ((ObjectNode) config).put("password", "fake");
    final AirbyteConnectionStatus actual = source.check(config);
    assertEquals(Status.FAILED, actual.getStatus());
    assertEquals(INCORRECT_USERNAME_OR_PASSWORD.getValue(), actual.getMessage());
  }

  @Test
  public void testCheckIncorrectUsernameFailure() throws Exception {
      ((ObjectNode) config).put("username", "fake");
      final AirbyteConnectionStatus actual = source.check(config);
      assertEquals(Status.FAILED, actual.getStatus());
      assertEquals(INCORRECT_USERNAME_OR_PASSWORD.getValue(), actual.getMessage());
  }

  @Test
  public void testCheckEmptyUsernameFailure() throws Exception {
    ((ObjectNode) config).put("username", "");
    final AirbyteConnectionStatus actual = source.check(config);
    assertEquals(Status.FAILED, actual.getStatus());
    assertEquals(INCORRECT_USERNAME_OR_HOST.getValue(), actual.getMessage());
  }

  @Test
  public void testCheckIncorrectHostFailure() throws Exception {
      ((ObjectNode) config).put("host", "localhost2");
      final AirbyteConnectionStatus actual = source.check(config);
      assertEquals(Status.FAILED, actual.getStatus());
      assertEquals(INCORRECT_USERNAME_OR_HOST.getValue(), actual.getMessage());
  }
}
