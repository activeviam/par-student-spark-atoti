package io.atoti.spark.aggregation;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;

import java.util.Objects;
import org.apache.spark.sql.Column;

public class Avg implements AggregatedValue {
  String name;
  String column;

  public Avg(String name, String column) {
    Objects.requireNonNull(name, "No name provided");
    Objects.requireNonNull(column, "No column provided");
    this.name = name;
    this.column = column;
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
