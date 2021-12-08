package io.atoti.spark;

import io.atoti.spark.condition.AndCondition;
import io.atoti.spark.condition.EqualCondition;
import io.atoti.spark.condition.FalseCondition;
import io.atoti.spark.condition.NotCondition;
import io.atoti.spark.condition.NullCondition;
import io.atoti.spark.condition.OrCondition;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

public class TestListQuery {

  SparkSession spark =
          SparkSession.builder().appName("Spark Atoti").config("spark.master", "local").getOrCreate();

  public TestListQuery() {
	  this.spark.sparkContext().setLogLevel("ERROR");
  }
  
  @Test
  void testListAllDataFrame() throws URISyntaxException {
    final Dataset<Row> dataframe = CsvReader.read("csv/basic.csv", spark);
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
  void testListFirstRows() throws URISyntaxException {
    final Dataset<Row> dataframe = CsvReader.read("csv/basic.csv", spark);
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
  void testListLastRow() throws URISyntaxException {
    final Dataset<Row> dataframe = CsvReader.read("csv/basic.csv", spark);
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
  void testNoRow() throws URISyntaxException {
    final Dataset<Row> dataframe = CsvReader.read("csv/basic.csv", spark);
    final var rows = ListQuery.list(dataframe, List.of("id", "value"), 0, 0);
    assertThat(rows).hasSize(0);
  }

  @Test
  void testTooBigLimit() throws URISyntaxException {
    final Dataset<Row> dataframe = CsvReader.read("csv/basic.csv", spark);
    final var rows = ListQuery.list(dataframe, List.of("id", "value"), 5, 0);
    assertThat(rows).hasSize(3);
  }

  @Test
  void testTooBigOffset() throws URISyntaxException {
    final Dataset<Row> dataframe = CsvReader.read("csv/basic.csv", spark);
    final var rows = ListQuery.list(dataframe, List.of("id", "value"), -1, 5);
    assertThat(rows).hasSize(0);
  }

  @Test
  void testNegativeOffset() throws URISyntaxException {
    final Dataset<Row> dataframe = CsvReader.read("csv/basic.csv", spark);
    assertThatThrownBy(() -> ListQuery.list(dataframe, List.of("id", "value"), 3, -2))
            .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void testTooBigOffsetWithLimit() throws URISyntaxException {
    final Dataset<Row> dataframe = CsvReader.read("csv/basic.csv", spark);
    final var rows = ListQuery.list(dataframe, List.of("id", "value"), 2, 5);
    assertThat(rows).hasSize(0);
  }

  @Test
  void testTooBigLimitWithOffset() throws URISyntaxException {
    final Dataset<Row> dataframe = CsvReader.read("csv/basic.csv", spark);
    final var rows = ListQuery.list(dataframe, List.of("id", "value"), 5, 2);
    assertThat(rows).hasSize(1);
  }

  @Test
  void testListWithCondition() throws URISyntaxException {
    final Dataset<Row> dataframe = CsvReader.read("csv/basic.csv", spark);
    final var rows = ListQuery.list(dataframe, new EqualCondition("id", 3));
    assertThat(rows).hasSize(1).extracting(rowReader("value")).first().isEqualTo(-420d);
  }

  @Test
  void testListWithComplexCondition() throws URISyntaxException {
    final Dataset<Row> dataframe = CsvReader.read("csv/basic.csv", spark);
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
	  final Dataset<Row> dataframe = CsvReader.read("csv/basic.csv", spark);
	    final var rows =
	        ListQuery.list(
	            dataframe,
	            FalseCondition.value());
	    assertThat(rows).isEmpty();
  }

  static Object readRowValue(final Row row, final String column) {
    return row.getAs(column);
  }

  @SuppressWarnings("unchecked")
static <T> Function<Object, T> rowReader(final String column) {
    return row -> (T) readRowValue((Row) row, column);
  }
}
