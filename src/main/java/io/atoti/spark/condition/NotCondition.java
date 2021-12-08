package io.atoti.spark.condition;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;

public record NotCondition(QueryCondition condition) implements QueryCondition {

  @Override
  public FilterFunction<Row> getCondition() {
    return (Row row) -> !this.condition.getCondition().call(row);
  }
}
