package io.atoti.spark.aggregation;

import org.apache.spark.sql.Column;

import java.util.Objects;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;

public record Max(String name, String column) implements AggregatedValue {

  public Max {
    Objects.requireNonNull(name, "No name provided");
    Objects.requireNonNull(column, "No column provided");
  }

  public Column getAggregateColumn() {
    return max(column).alias(name);
  }

  public Column getName() {
    return col(name);
  }
}
