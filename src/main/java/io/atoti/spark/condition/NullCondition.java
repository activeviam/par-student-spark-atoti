package io.atoti.spark.condition;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;

public record NullCondition(String fieldName) implements QueryCondition {

  @Override
  public FilterFunction<Row> getCondition() {
    return (Row row) -> (row.isNullAt(row.fieldIndex(fieldName)));
  }

  @Override
  public String toSqlQuery() {
    return "NULL";
  }
}
