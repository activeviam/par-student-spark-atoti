package io.atoti.spark;

import static org.assertj.core.api.Assertions.assertThat;

import io.atoti.spark.aggregation.Avg;
import io.atoti.spark.aggregation.Count;
import io.atoti.spark.aggregation.Max;
import io.atoti.spark.aggregation.Sum;
import io.atoti.spark.condition.AndCondition;
import io.atoti.spark.condition.EqualCondition;
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

class TestSqlQuery {

  static SparkSession spark =
      SparkSession.builder().appName("Spark Atoti").config("spark.master", "local").getOrCreate();

  public TestSqlQuery() {
    spark.sparkContext().setLogLevel("ERROR");

	registerCsvAsSqlView("csv/basic.csv", "basic");
	registerCsvAsSqlView("csv/calculate.csv", "calculate");
	registerCsvAsSqlView("csv/toJoin.csv", "toJoin");
	registerCsvAsSqlView("csv/twoTypesInSameColumn.csv", "twoTypesInSameColumn");
  }
  
 private static void registerCsvAsSqlView(String fileName, String tableName) {
	 final Dataset<Row> dataframe = CsvReader.read(fileName, spark);
	 dataframe.createOrReplaceTempView(tableName);
 }

  @Test
  void testListAllDataFrame() {
    final List<Row> rows = ListQuery.listSql(spark, "basic", List.of("id", "value"), -1, 0);
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
  void testListWithComplexCondition() {
    final var rows =
        ListQuery.listSql(
            spark,
            "basic",
            AndCondition.of(
                new EqualCondition("label", "a"),
                new NotCondition(
                    OrCondition.of(new EqualCondition("id", 1), new NullCondition("value")))));
    assertThat(rows).hasSize(1).extracting(rowReader("id")).first().isEqualTo(2);
  }
  
  @Test
  void testBasicAggregation() {
    final var rows =
        AggregateQuery.aggregateSql(
                spark,
                "basic",
                List.of("label"),
                List.of(
                    new Count("c"),
                    new Sum("s", "value"),
                    new Max("M", "id"),
                    new Avg("avg", "value")))
            .collectAsList();

    // result must have 2 values
    assertThat(rows).hasSize(2);

    final var rowsByLabel =
        rows.stream()
            .collect(Collectors.toUnmodifiableMap(row -> (row.getAs("label")), row -> (row)));

    // for label = a -> c = 2, s = 25.91, M = 2
    final var rowA = rowsByLabel.get("a");
    assertThat((long) rowA.getAs("c")).isEqualTo(2);
    assertThat((double) rowA.getAs("s")).isEqualTo(25.91);
    assertThat((int) rowA.getAs("M")).isEqualTo(2);
    assertThat((double) rowA.getAs("avg")).isEqualTo(12.955);

    // for label = b -> c = 1, s = -420, M = 3
    final var rowB = rowsByLabel.get("b");
    assertThat((long) rowB.getAs("c")).isEqualTo(1);
    assertThat((double) rowB.getAs("s")).isEqualTo(-420);
    assertThat((int) rowB.getAs("M")).isEqualTo(3);
  }

  @Test
  void testAggregateWithCondition() {
    final var rows =
        AggregateQuery.aggregateSql(
                spark, "basic", List.of("id"), List.of(new Count("c")), new EqualCondition("label", "a"))
            .collectAsList();

    // result must have 2 values
    assertThat(rows).hasSize(2);

    final var rowsById =
        rows.stream().collect(Collectors.toUnmodifiableMap(row -> (row.getAs("id")), row -> (row)));

    // id = 1 -> c = 1
    final var row1 = rowsById.get(1);
    assertThat((long) row1.getAs("c")).isEqualTo(1);

    // id = 2 -> c = 1
    final var row2 = rowsById.get(2);
    assertThat((long) row2.getAs("c")).isEqualTo(1);
  }
  
//  @Test
//  void testListFirstRows() {
//    final Dataset<Row> dataframe = CsvReader.read("csv/basic.csv", spark);
//    final var rows = ListQuery.list(dataframe, List.of("id", "value"), 2, 0);
//    assertThat(rows).hasSize(2);
//    final var valuesById =
//        rows.stream()
//            .collect(
//                Collectors.toUnmodifiableMap(
//                    row -> ((Number) readRowValue(row, "id")).longValue(),
//                    row -> ((Number) readRowValue(row, "value")).doubleValue()));
//    assertThat(valuesById).containsExactlyEntriesOf(Map.of(1L, 12.34d, 2L, 13.57d));
//  }
//
//  @Test
//  void testListLastRow() {
//    final Dataset<Row> dataframe = CsvReader.read("csv/basic.csv", spark);
//    final var rows = ListQuery.list(dataframe, List.of("id", "value"), 1, 2);
//    assertThat(rows).hasSize(1);
//    final var valuesById =
//        rows.stream()
//            .collect(
//                Collectors.toUnmodifiableMap(
//                    row -> ((Number) readRowValue(row, "id")).longValue(),
//                    row -> ((Number) readRowValue(row, "value")).doubleValue()));
//    assertThat(valuesById).containsExactlyEntriesOf(Map.of(3L, -420d));
//  }
//
//  @Test
//  void testListWithCondition() {
//    final Dataset<Row> dataframe = CsvReader.read("csv/basic.csv", spark);
//    final var rows = ListQuery.list(dataframe, new EqualCondition("id", 3));
//    System.out.println(rows);
//    assertThat(rows).hasSize(1).extracting(rowReader("value")).first().isEqualTo(-420d);
//  }
//
//  @Test
//  void testListWithComplexCondition() {
//    final Dataset<Row> dataframe = CsvReader.read("csv/basic.csv", spark);
//    final var rows =
//        ListQuery.list(
//            dataframe,
//            AndCondition.of(
//                new EqualCondition("label", "a"),
//                new NotCondition(
//                    OrCondition.of(new EqualCondition("id", 1), new NullCondition("value")))));
//    assertThat(rows).hasSize(1).extracting(rowReader("id")).first().isEqualTo(2);
//  }
//
  static Object readRowValue(final Row row, final String column) {
    return row.getAs(column);
  }

  @SuppressWarnings("unchecked")
  static <T> Function<Object, T> rowReader(final String column) {
    return row -> (T) readRowValue((Row) row, column);
  }
}
