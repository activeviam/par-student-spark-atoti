package io.atoti.spark.aggregation;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.min;

import java.util.Objects;
import org.apache.spark.sql.Column;

public class Min implements AggregatedValue {
  String name;
  String column;

  public Min(String name, String column) {
    Objects.requireNonNull(name, "No name provided");
    Objects.requireNonNull(column, "No column provided");
    this.name = name;
    this.column = column;
  }

  public Column toAggregateColumn() {
    return min(column).alias(name);
  }

  public Column toColumn() {
    return col(name);
  }

  public String toSqlQuery() {
    return "MIN(" + column + ") AS " + name;
  }
}
