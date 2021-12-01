package io.atoti.spark;

import io.atoti.spark.aggregation.AggregatedValue;
import io.atoti.spark.condition.QueryCondition;
import java.util.List;

public class AggregateQuery {

  /**
   * Retrieves the aggregations of rows from a dataframe.
   *
   * @param groupByColumns names of the columns to consider to group-by the provided dataframe
   * @param aggregations aggregated values to return with the result
   */
  public static Object aggregate(
      Object dataframe,
      List<String> groupByColumns,
      List<AggregatedValue> aggregations,
      QueryCondition condition) {
    // The result can be a dataframe
    throw new UnsupportedOperationException("TODO");
  }
}
