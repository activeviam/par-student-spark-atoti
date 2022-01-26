package io.atoti.spark.condition;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

public record NotCondition(QueryCondition condition) implements QueryCondition {

  @Override
  public Column getCondition() {
    return functions.not(condition.getCondition());
  }

  @Override
  public String toSqlQuery() {
    return "! (" + this.condition.toSqlQuery() + ")";
  }
}
