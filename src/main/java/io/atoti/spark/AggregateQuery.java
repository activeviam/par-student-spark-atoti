package io.atoti.spark;

import io.atoti.spark.aggregation.AggregatedValue;
import io.atoti.spark.condition.QueryCondition;

import java.util.List;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class AggregateQuery {

  /**
   * Retrieves the aggregations of rows from a dataframe.
   *
   * @param groupByColumns names of the columns to consider to group-by the provided dataframe
   * @param aggregations aggregated values to return with the result
   */
  public static Dataset<Row> aggregate(
      Dataset<Row> dataframe,
      List<String> groupByColumns,
      List<AggregatedValue> aggregations,
      QueryCondition condition) {
	  final Column[] columns = groupByColumns.stream().map(functions::col).toArray(Column[]::new);
	  final Column firstAggColumn = aggregations.get(0).getAggregateColumn(); 
	  final Column[] nextAggColumns =
			  aggregations.subList(1, aggregations.size()).stream()
			  	.map(agg -> agg.getAggregateColumn()).toArray(Column[]::new);
	  return dataframe.select(columns).groupBy(columns).agg(firstAggColumn, nextAggColumns).filter(condition.getCondition());
  }
}
