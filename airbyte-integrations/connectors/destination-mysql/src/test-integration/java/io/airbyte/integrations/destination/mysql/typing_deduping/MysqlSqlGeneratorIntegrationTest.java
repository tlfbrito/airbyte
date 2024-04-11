 /*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.mysql.typing_deduping;

 import static io.airbyte.cdk.integrations.base.JavaBaseConstants.COLUMN_NAME_AB_EXTRACTED_AT;
 import static io.airbyte.cdk.integrations.base.JavaBaseConstants.COLUMN_NAME_AB_ID;
 import static io.airbyte.cdk.integrations.base.JavaBaseConstants.COLUMN_NAME_AB_LOADED_AT;
 import static io.airbyte.cdk.integrations.base.JavaBaseConstants.COLUMN_NAME_AB_RAW_ID;
 import static io.airbyte.cdk.integrations.base.JavaBaseConstants.COLUMN_NAME_DATA;
 import static io.airbyte.cdk.integrations.base.JavaBaseConstants.COLUMN_NAME_EMITTED_AT;
 import static io.airbyte.integrations.destination.mysql.typing_deduping.MysqlSqlGenerator.TIMESTAMP_FORMATTER;
 import static org.junit.jupiter.api.Assertions.assertEquals;
 import static org.junit.jupiter.api.Assertions.assertFalse;
 import static org.junit.jupiter.api.Assertions.assertTrue;

 import com.fasterxml.jackson.databind.JsonNode;
 import com.fasterxml.jackson.databind.node.ObjectNode;
 import io.airbyte.cdk.db.jdbc.DefaultJdbcDatabase;
 import io.airbyte.cdk.db.jdbc.JdbcDatabase;
 import io.airbyte.cdk.db.jdbc.JdbcSourceOperations;
 import io.airbyte.cdk.integrations.destination.jdbc.typing_deduping.JdbcSqlGenerator;
 import io.airbyte.cdk.integrations.standardtest.destination.typing_deduping.JdbcSqlGeneratorIntegrationTest;
 import io.airbyte.commons.json.Jsons;
 import io.airbyte.integrations.base.destination.typing_deduping.DestinationHandler;
 import io.airbyte.integrations.base.destination.typing_deduping.DestinationInitialStatus;
 import io.airbyte.integrations.base.destination.typing_deduping.Sql;
 import io.airbyte.integrations.base.destination.typing_deduping.StreamId;
 import io.airbyte.integrations.base.destination.typing_deduping.migrators.MinimumDestinationState;
 import io.airbyte.integrations.destination.mysql.MySQLDestination;
 import io.airbyte.integrations.destination.mysql.MySQLDestinationAcceptanceTest;
 import io.airbyte.integrations.destination.mysql.MySQLNameTransformer;
 import java.sql.ResultSet;
 import java.sql.SQLException;
 import java.time.OffsetDateTime;
 import java.util.List;
 import javax.sql.DataSource;
 import org.jooq.DataType;
 import org.jooq.Field;
 import org.jooq.SQLDialect;
 import org.jooq.conf.ParamType;
 import org.jooq.impl.DSL;
 import org.jooq.impl.DefaultDataType;
 import org.jooq.impl.SQLDataType;
 import org.junit.jupiter.api.AfterAll;
 import org.junit.jupiter.api.BeforeAll;
 import org.junit.jupiter.api.Test;
 import org.testcontainers.containers.MySQLContainer;

public class MysqlSqlGeneratorIntegrationTest extends JdbcSqlGeneratorIntegrationTest<MinimumDestinationState> {

  private static MySQLContainer<?> testContainer;
  private static JdbcDatabase database;

  public static class MysqlSourceOperations extends JdbcSourceOperations {

    @Override
    public void copyToJsonField(final ResultSet resultSet, final int colIndex, final ObjectNode json) throws SQLException {
      final String columnName = resultSet.getMetaData().getColumnName(colIndex);
      final String columnTypeName = resultSet.getMetaData().getColumnTypeName(colIndex).toLowerCase();

      // JSONB has no equivalent in JDBCType
      if ("json".equals(columnTypeName)) {
        json.set(columnName, Jsons.deserializeExact(resultSet.getString(colIndex)));
      } else {
        super.copyToJsonField(resultSet, colIndex, json);
      }
    }

  }

  @BeforeAll
  public static void setupMysql() throws Exception {
    testContainer = new MySQLContainer<>("mysql:8.0");
    testContainer.start();
    MySQLDestinationAcceptanceTest.configureTestContainer(testContainer);

    final JsonNode config = MySQLDestinationAcceptanceTest.getConfigFromTestContainer(testContainer);

    // TODO move this into JdbcSqlGeneratorIntegrationTest?
    // This code was largely copied from RedshiftSqlGeneratorIntegrationTest
    // TODO: Its sad to instantiate unneeded dependency to construct database and datsources. pull it to
    // static methods.
    final MySQLDestination insertDestination = new MySQLDestination();
    final DataSource dataSource = insertDestination.getDataSource(config);
    database = new DefaultJdbcDatabase(dataSource, new MysqlSourceOperations());
  }

  @AfterAll
  public static void teardownMysql() {
    testContainer.stop();
    testContainer.close();
  }

  @Override
  protected JdbcSqlGenerator getSqlGenerator() {
    return new MysqlSqlGenerator(new MySQLNameTransformer());
  }

  @Override
  protected DestinationHandler<MinimumDestinationState> getDestinationHandler() {
    // Mysql doesn't have an actual schema concept.
    // All of our queries pass a value into the "schemaName" parameter, which mysql treats as being
    // the database name.
    // So we pass null for the databaseName parameter here, because we don't use the 'test' database at all.
    return new MysqlDestinationHandler(null, database, getNamespace());
  }

  @Override
  public void insertRawTableRecords(final StreamId streamId, final List<? extends JsonNode> records) throws Exception {
    reformatMetaColumnTimestamps(records);
    super.insertRawTableRecords(streamId, records);
  }

  @Override
  protected void insertFinalTableRecords(final boolean includeCdcDeletedAt,
                                         final StreamId streamId,
                                         final String suffix,
                                         final List<? extends JsonNode> records)
      throws Exception {
    reformatMetaColumnTimestamps(records);
    super.insertFinalTableRecords(includeCdcDeletedAt, streamId, suffix, records);
  }

  @Override
  protected void insertV1RawTableRecords(final StreamId streamId, final List<? extends JsonNode> records) throws Exception {
    reformatMetaColumnTimestamps(records);
    super.insertV1RawTableRecords(streamId, records);
  }

  private static void reformatMetaColumnTimestamps(final List<? extends JsonNode> records) {
    // We use mysql's TIMESTAMP(6) type for extracted_at+loaded_at.
    // Unfortunately, mysql doesn't allow you to use the 'Z' suffix for UTC timestamps.
    // Convert those to '+00:00' here.
    for (final JsonNode record : records) {
      reformatTimestampIfPresent(record, COLUMN_NAME_AB_EXTRACTED_AT);
      reformatTimestampIfPresent(record, COLUMN_NAME_EMITTED_AT);
      reformatTimestampIfPresent(record, COLUMN_NAME_AB_LOADED_AT);
    }
  }

  private static void reformatTimestampIfPresent(final JsonNode record, final String columnNameAbExtractedAt) {
    if (record.has(columnNameAbExtractedAt)) {
      final OffsetDateTime extractedAt = OffsetDateTime.parse(record.get(columnNameAbExtractedAt).asText());
      final String reformattedExtractedAt = TIMESTAMP_FORMATTER.format(extractedAt);
      ((ObjectNode) record).put(columnNameAbExtractedAt, reformattedExtractedAt);
    }
  }

  @Override
  protected void createRawTable(final StreamId streamId) throws Exception {
    getDatabase().execute(getDslContext().createTable(DSL.name(streamId.getRawNamespace(), streamId.getRawName()))
        .column(COLUMN_NAME_AB_RAW_ID, SQLDataType.VARCHAR(36).nullable(false))
        // we use VARCHAR for timestamp values, but TIMESTAMP(6) for extracted+loaded_at.
        // because legacy normalization did that. :shrug:
        .column(COLUMN_NAME_AB_EXTRACTED_AT, SQLDataType.TIMESTAMP(6).nullable(false))
        .column(COLUMN_NAME_AB_LOADED_AT, SQLDataType.TIMESTAMP(6))
        .column(COLUMN_NAME_DATA, getStructType().nullable(false))
        .getSQL(ParamType.INLINED));
  }

  @Override
  protected void createV1RawTable(final StreamId v1RawTable) throws Exception {
    getDatabase().execute(getDslContext().createTable(DSL.name(v1RawTable.getRawNamespace(), v1RawTable.getRawName()))
        .column(COLUMN_NAME_AB_ID, SQLDataType.VARCHAR(36).nullable(false))
        // similar to createRawTable - this data type is timestmap, not varchar
        .column(COLUMN_NAME_EMITTED_AT, SQLDataType.TIMESTAMP(6).nullable(false))
        .column(COLUMN_NAME_DATA, getStructType().nullable(false))
        .getSQL(ParamType.INLINED));
  }

  @Test
  @Override
  public void testCreateTableIncremental() throws Exception {
    // TODO
    final Sql sql = getGenerator().createTable(getIncrementalDedupStream(), "", false);
    getDestinationHandler().execute(sql);

    List<DestinationInitialStatus<MinimumDestinationState>> initialStatuses = getDestinationHandler().gatherInitialState(List.of(getIncrementalDedupStream()));
    assertEquals(1, initialStatuses.size());
    final DestinationInitialStatus<MinimumDestinationState> initialStatus = initialStatuses.getFirst();
    assertTrue(initialStatus.isFinalTablePresent());
    assertFalse(initialStatus.isSchemaMismatch());
  }

  @Override
  protected JdbcDatabase getDatabase() {
    return database;
  }

  @Override
  protected DataType<?> getStructType() {
    return new DefaultDataType<>(null, String.class, "json");
  }

  @Override
  protected SQLDialect getSqlDialect() {
    return SQLDialect.MYSQL;
  }

  @Override
  protected Field<?> toJsonValue(final String valueAsString) {
    // mysql lets you just insert json strings directly into json columns
    return DSL.val(valueAsString);
  }

  @Override
  protected void teardownNamespace(final String namespace) throws Exception {
    // mysql doesn't have a CASCADE keyword in DROP SCHEMA, so we have to override this method.
    // we're currently on jooq 3.13; jooq's dropDatabase() call was only added in 3.14
    getDatabase().execute(getDslContext().dropSchema(namespace).getSQL(ParamType.INLINED));
  }

  @Override
  protected boolean getSupportsSafeCast() {
    return false;
  }

}
