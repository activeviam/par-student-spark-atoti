package io.atoti.spark.aggregation;

import java.util.Objects;
import org.apache.spark.sql.Column;

public record Multiply(String name, AggregatedValue lhs, AggregatedValue rhs) implements AggregatedValue {

  public Multiply {
    Objects.requireNonNull(name, "No name provided");
    Objects.requireNonNull(lhs, "No left-hand value provided");
    Objects.requireNonNull(rhs, "No right-hand value provided");
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
