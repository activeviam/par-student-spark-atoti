package io.atoti.spark.aggregation;

import java.util.Objects;

public record Sum(String name, String column) implements AggregatedValue {

  public Sum {
    Objects.requireNonNull(name, "No name provided");
    Objects.requireNonNull(column, "No column provided");
  }
}
