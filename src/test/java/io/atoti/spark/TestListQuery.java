package io.atoti.spark;

import static org.assertj.core.api.Assertions.*;

import io.atoti.spark.condition.AndCondition;
import io.atoti.spark.condition.EqualCondition;
import io.atoti.spark.condition.FalseCondition;
import io.atoti.spark.condition.NotCondition;
import io.atoti.spark.condition.NullCondition;
import io.atoti.spark.condition.OrCondition;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

class TestListQuery {

  SparkSession spark =
      SparkSession.builder().appName("Spark Atoti").config("spark.master", "local").getOrCreate();

  public TestListQuery() {
    this.spark.sparkContext().setLogLevel("ERROR");
    spark.sparkContext().addJar("./out/artifacts/condition/condition.jar");
  }

  @Test
  void testListAllDataFrame() {
    final Dataset<Row> dataframe = spark.read().table("basic");
    final List<Row> rows = ListQuery.list(dataframe, List.of("id", "value"), -1, 0);
    assertThat(rows).hasSize(3);
    final var valuesById =
        rows.stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    row -> ((Number) readRowValue(row, "id")).longValue(),
                    row -> ((Number) readRowValue(row, "value")).doubleValue()));
    assertThat(valuesById).containsExactlyEntriesOf(Map.of(1L, 12.34d, 2L, 13.57d, 3L, -420d));
  }

  @Test
  void testListFirstRows() {
    final Dataset<Row> dataframe = spark.read().table("basic");
    final var rows = ListQuery.list(dataframe, List.of("id", "value"), 2, 0);
    assertThat(rows).hasSize(2);
    final var valuesById =
        rows.stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    row -> ((Number) readRowValue(row, "id")).longValue(),
                    row -> ((Number) readRowValue(row, "value")).doubleValue()));
    assertThat(valuesById).containsExactlyEntriesOf(Map.of(1L, 12.34d, 2L, 13.57d));
  }

  @Test
  void testListLastRow() {
    final Dataset<Row> dataframe = spark.read().table("basic");
    final var rows = ListQuery.list(dataframe, List.of("id", "value"), 1, 2);
    assertThat(rows).hasSize(1);
    final var valuesById =
        rows.stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    row -> ((Number) readRowValue(row, "id")).longValue(),
                    row -> ((Number) readRowValue(row, "value")).doubleValue()));
    assertThat(valuesById).containsExactlyEntriesOf(Map.of(3L, -420d));
  }

  @Test
  void testNoRow() {
    final Dataset<Row> dataframe = spark.read().table("basic");
    final var rows = ListQuery.list(dataframe, List.of("id", "value"), 0, 0);
    assertThat(rows).hasSize(0);
  }

  @Test
  void testTooBigLimit() {
    final Dataset<Row> dataframe = spark.read().table("basic");
    final var rows = ListQuery.list(dataframe, List.of("id", "value"), 5, 0);
    assertThat(rows).hasSize(3);
  }

  @Test
  void testTooBigOffset() {
    final Dataset<Row> dataframe = spark.read().table("basic");
    final var rows = ListQuery.list(dataframe, List.of("id", "value"), -1, 5);
    assertThat(rows).hasSize(0);
  }

  @Test
  void testNegativeOffset() {
    final Dataset<Row> dataframe = spark.read().table("basic");
    assertThatThrownBy(() -> ListQuery.list(dataframe, List.of("id", "value"), 3, -2))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void testTooBigOffsetWithLimit() {
    final Dataset<Row> dataframe = spark.read().table("basic");
    final var rows = ListQuery.list(dataframe, List.of("id", "value"), 2, 5);
    assertThat(rows).hasSize(0);
  }

  @Test
  void testTooBigLimitWithOffset() {
    final Dataset<Row> dataframe = spark.read().table("basic");
    final var rows = ListQuery.list(dataframe, List.of("id", "value"), 5, 2);
    assertThat(rows).hasSize(1);
  }

  @Test
  void testListWithCondition() {
    final Dataset<Row> dataframe = spark.read().table("basic");
    final var rows = ListQuery.list(dataframe, new EqualCondition("id", 3));
    assertThat(rows).hasSize(1).extracting(rowReader("value")).first().isEqualTo(-420d);
  }

  @Test
  void testListWithComplexCondition() {
    final Dataset<Row> dataframe = spark.read().table("basic");
    final var rows =
        ListQuery.list(
            dataframe,
            AndCondition.of(
                new EqualCondition("label", "a"),
                new NotCondition(
                    OrCondition.of(new EqualCondition("id", 1), new NullCondition("value")))));
    assertThat(rows).hasSize(1).extracting(rowReader("id")).first().isEqualTo(2);
  }

  @Test
  void testListWithFalseCondition() {
    final Dataset<Row> dataframe = spark.read().table("basic");
    final var rows = ListQuery.list(dataframe, FalseCondition.value());
    assertThat(rows).isEmpty();
  }

  @Test
  void testListWithConditionAndLimit() {
    final Dataset<Row> dataframe = spark.read().table("basic");
    final var rows = ListQuery.list(dataframe, List.of("id", "value"), new EqualCondition("id", 3), 1, 0);
    assertThat(rows).hasSize(1);
    final var valuesById =
        rows.stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    row -> ((Number) readRowValue(row, "id")).longValue(),
                    row -> ((Number) readRowValue(row, "value")).doubleValue()));
    assertThat(valuesById).containsExactlyEntriesOf(Map.of(3L, -420d));
  }

  @Test
  void testListWithConditionAndOffset() {
    final Dataset<Row> dataframe = spark.read().table("basic");
    final var rows = ListQuery.list(dataframe, List.of("id", "value"), new NotCondition(new EqualCondition("id", 3)), -1, 1);
    assertThat(rows).hasSize(1);
    final var valuesById =
        rows.stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    row -> ((Number) readRowValue(row, "id")).longValue(),
                    row -> ((Number) readRowValue(row, "value")).doubleValue()));
    assertThat(valuesById).containsExactlyEntriesOf(Map.of(2L, 13.57d));
  }

  static Object readRowValue(final Row row, final String column) {
    return row.getAs(column);
  }

  @SuppressWarnings("unchecked")
  static <T> Function<Object, T> rowReader(final String column) {
    return row -> (T) readRowValue((Row) row, column);
  }
}
