package io.atoti.spark.aggregation;

import java.util.Objects;
import org.apache.spark.sql.Column;

public record Quantile(String name, AggregatedValue vectorValue, float percent) implements AggregatedValue {

  public Quantile {
    Objects.requireNonNull(name, "No name provided");
    Objects.requireNonNull(vectorValue, "No vector value provided");
    if (percent < 0 || percent > 100) {
      throw new IllegalArgumentException("Percent must be 0 <= p <= 100");
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
