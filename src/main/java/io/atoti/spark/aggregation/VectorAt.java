package io.atoti.spark.aggregation;

import java.util.Objects;
import org.apache.spark.sql.Column;

public record VectorAt(String name, AggregatedValue vectorValue, int position) implements AggregatedValue {

  public VectorAt {
    Objects.requireNonNull(name, "No name provided");
    Objects.requireNonNull(vectorValue, "No vector value provided");
    if (position < 0) {
      throw new IllegalArgumentException("Position must be positive or null");
    }
  }

  public Column toAggregateColumn() {
    throw new UnsupportedOperationException("TODO");
  }

  public Column toColumn() {
    throw new UnsupportedOperationException("TODO");
  }

  public String toSqlQuery() {
    throw new UnsupportedOperationException("TODO");
  }
}
