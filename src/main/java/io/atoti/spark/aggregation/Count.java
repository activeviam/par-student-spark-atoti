package io.atoti.spark.aggregation;

import static org.apache.spark.sql.functions.*;

import java.util.Objects;
import org.apache.spark.sql.Column;

/**
 * Count is a special function very standard in atoti that returns the number of rows that have been
 * aggregated for a given group-by.
 *
 * <p>Consider the following DataFrame:<br>
 *
 * <pre>
 * | A  | B  |
 * | -- | -- |
 * | a1 | b1 |
 * | a1 | b2 |
 * | a2 | b1 |
 * </pre>
 *
 * When computing Count for this DataFrame grouped by A, we get the following result:
 *
 * <pre>
 * | A  | Count |
 * | -- | ----- |
 * | a1 |   2   |
 * | a2 |   1   |
 * </pre>
 */
public class Count implements AggregatedValue {
  String name;

  public Count(String name) {
    Objects.requireNonNull(name, "No name provided");
    this.name = name;
  }

  public Column toAggregateColumn() {
    return count(lit(1)).alias(name);
  }

  public Column toColumn() {
    return col(name);
  }

  public String toSqlQuery() {
    return "COUNT(*) AS " + name;
  }
}
