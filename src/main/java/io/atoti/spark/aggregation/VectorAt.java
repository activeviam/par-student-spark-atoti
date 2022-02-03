package io.atoti.spark.aggregation;

import java.util.Objects;
import org.apache.spark.sql.Column;

import static org.apache.spark.sql.functions.element_at;
import static org.apache.spark.sql.functions.col;

public record VectorAt(String name, AggregatedValue vectorValue, int position) implements AggregatedValue {

  public VectorAt {
    Objects.requireNonNull(name, "No name provided");
    Objects.requireNonNull(vectorValue, "No vector value provided");
    if (position < 0) {
      throw new IllegalArgumentException("Position must be positive or null");
    }
  }

  public Column toAggregateColumn() {
    return element_at(vectorValue.toColumn(), position).alias(name);
  }

  public Column toColumn() {
    return col(name);
  }

  public String toSqlQuery() {
    throw new UnsupportedOperationException("TODO");
  }
}
