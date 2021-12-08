package io.atoti.spark.aggregation;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;

import java.util.Objects;
import org.apache.spark.sql.Column;

public record Max(String name, String column) implements AggregatedValue {

  public Max {
    Objects.requireNonNull(name, "No name provided");
    Objects.requireNonNull(column, "No column provided");
  }

  public Column toAggregateColumn() {
    return max(column).alias(name);
  }

  public Column toColumn() {
    return col(name);
  }
}
