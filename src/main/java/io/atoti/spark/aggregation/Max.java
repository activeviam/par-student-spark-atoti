package io.atoti.spark.aggregation;

import java.util.Objects;

public record Max(String name, String column) implements AggregatedValue {

  public Max {
    Objects.requireNonNull(name, "No name provided");
    Objects.requireNonNull(column, "No column provided");
  }
}
