package io.atoti.spark.aggregation;

import java.util.Objects;

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
public record Count(String name) implements AggregatedValue {
  public Count {
    Objects.requireNonNull(name, "No name provided");
  }
}
