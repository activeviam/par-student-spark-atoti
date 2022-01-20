package io.atoti.spark.condition;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

public class NotCondition implements QueryCondition {
  QueryCondition condition;

  public NotCondition(QueryCondition condition) {
    this.condition = condition;
  }

  @Override
  public Column getCondition() {
    return functions.not(condition.getCondition());
  }

  @Override
  public String toSqlQuery() {
    return "! (" + this.condition.toSqlQuery() + ")";
  }
}
