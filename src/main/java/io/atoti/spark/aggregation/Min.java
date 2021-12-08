package io.atoti.spark.aggregation;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.min;

import java.util.Objects;
import org.apache.spark.sql.Column;

public record Min(String name, String column) implements AggregatedValue {

  public Min {
    Objects.requireNonNull(name, "No name provided");
    Objects.requireNonNull(column, "No column provided");
  }

  public Column toAggregateColumn() {
    return min(column).alias(name);
  }

  public Column toColumn() {
    return col(name);
  }
}
