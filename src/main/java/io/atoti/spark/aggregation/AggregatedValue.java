package io.atoti.spark.aggregation;

import org.apache.spark.sql.Column;

public sealed interface AggregatedValue permits Sum, Min, Max, Count {

  /**
   * Returns the name of the aggregated value.
   *
   * <p>This can be used to alias the created column, similarly to {@code SUM(p) AS sum_p} in a SQL
   * query.
   */
  Column getAggregateColumn();

  Column getName();
}
