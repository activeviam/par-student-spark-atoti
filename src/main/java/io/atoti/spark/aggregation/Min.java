package io.atoti.spark.aggregation;

import java.util.Objects;

public record Min(String name, String column) implements AggregatedValue {

  public Min {
    Objects.requireNonNull(name, "No name provided");
    Objects.requireNonNull(column, "No column provided");
  }
}
