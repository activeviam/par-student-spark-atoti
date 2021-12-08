package io.atoti.spark.aggregation;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;

import java.util.Objects;
import org.apache.spark.sql.Column;

public record Avg(String name, String column) implements AggregatedValue {

  public Avg {
    Objects.requireNonNull(name, "No name provided");
    Objects.requireNonNull(column, "No column provided");
  }

  public Column toAggregateColumn() {
    return avg(column).alias(name);
  }

  public Column toColumn() {
    return col(name);
  }

  public String toSqlQuery() {
    return "AVG(" + column + ") AS " + name;
  }
}
