package io.atoti.spark.aggregation;

import org.apache.spark.sql.Column;

import java.util.Objects;

import static org.apache.spark.sql.functions.min;

public record Min(String name, String column) implements AggregatedValue {

  public Min {
    Objects.requireNonNull(name, "No name provided");
    Objects.requireNonNull(column, "No column provided");
  }

  public Column getAggregateColumn() {
    return min(column).alias(name);
  }
}
