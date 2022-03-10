package io.atoti.spark.aggregation;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

import java.util.Objects;
import org.apache.spark.sql.Column;

public class Sum implements AggregatedValue {
  String name;
  String column;

  public Sum(String name, String column) {
    Objects.requireNonNull(name, "No name provided");
    Objects.requireNonNull(column, "No column provided");
    this.name = name;
    this.column = column;
  }

  public Column toAggregateColumn() {
    return sum(column).alias(name);
  }

  public Column toColumn() {
    return col(name);
  }

  public String toSqlQuery() {
    return "SUM(" + column + ") AS " + name;
  }
}
