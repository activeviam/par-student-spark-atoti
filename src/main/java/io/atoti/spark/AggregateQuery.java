package io.atoti.spark;

import io.atoti.spark.aggregation.AggregatedValue;
import io.atoti.spark.condition.QueryCondition;
import io.atoti.spark.condition.TrueCondition;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class AggregateQuery {

  /**
   * Retrieves the aggregations of rows from a dataframe.
   *
   * @param groupByColumns names of the columns to consider to group-by the provided dataframe
   * @param aggregations aggregated values to return with the result
   */
  public static Dataset<Row> aggregate(
      Dataset<Row> dataframe, List<String> groupByColumns, List<AggregatedValue> aggregations) {
    return aggregate(dataframe, groupByColumns, aggregations, TrueCondition.value());
  }

  public static Dataset<Row> aggregate(
      Dataset<Row> dataframe,
      List<String> groupByColumns,
      List<AggregatedValue> aggregations,
      QueryCondition condition) {
    if (aggregations.isEmpty()) {
      throw new IllegalArgumentException(
          "#aggregate can only be called with at least one AggregatedValue");
    }

    final Column[] columns = groupByColumns.stream().map(functions::col).toArray(Column[]::new);
    final Column[] createdColumns =
        aggregations.stream().map(AggregatedValue::toColumn).toArray(Column[]::new);
    final Column[] columnsToSelect = Arrays.copyOf(columns, columns.length + createdColumns.length);
    System.arraycopy(createdColumns, 0, columnsToSelect, columns.length, createdColumns.length);

    final Column firstAggColumn = aggregations.get(0).toAggregateColumn();
    final Column[] nextAggColumns =
        aggregations.subList(1, aggregations.size()).stream()
            .map(AggregatedValue::toAggregateColumn)
            .toArray(Column[]::new);
    return dataframe
        .filter(condition.getCondition())
        .groupBy(columns)
        .agg(firstAggColumn, nextAggColumns)
        .select(columnsToSelect);
  }

  public static Dataset<Row> aggregateSql(
      SparkSession spark,
      String table,
      List<String> groupByColumns,
      List<AggregatedValue> aggregations) {
    return aggregateSql(spark, table, groupByColumns, aggregations, TrueCondition.value());
  }

  public static Dataset<Row> aggregateSql(
      SparkSession spark,
      String table,
      List<String> groupByColumns,
      List<AggregatedValue> aggregations,
      QueryCondition condition) {
    if (aggregations.isEmpty()) {
      throw new IllegalArgumentException(
          "#aggregate can only be called with at least one AggregatedValue");
    }
    String[] aggregationsSql =
        aggregations.stream().map(AggregatedValue::toSqlQuery).toArray(String[]::new);
    String query =
        "SELECT "
            + String.join(", ", groupByColumns)
            + ", "
            + String.join(", ", aggregationsSql)
            + " FROM "
            + table
            + " WHERE "
            + condition.toSqlQuery()
            + " GROUP BY "
            + String.join(", ", groupByColumns)
            + ";";
    return spark.sql(query);
  }
}
