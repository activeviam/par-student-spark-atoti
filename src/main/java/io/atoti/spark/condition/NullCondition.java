package io.atoti.spark.condition;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

public record NullCondition(String fieldName) implements QueryCondition {

  @Override
  public Column getCondition() {
    return functions.col(fieldName).isNull();
  }

  @Override
  public String toSqlQuery() {
    return this.fieldName + " IS NULL";
  }
}
