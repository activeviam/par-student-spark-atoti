package io.atoti.spark.condition;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

public class NullCondition implements QueryCondition {
  String fieldName;

  public NullCondition(String fieldName) {
    this.fieldName = fieldName;
  }

  @Override
  public Column getCondition() {
    return functions.col(fieldName).isNull();
  }

  @Override
  public String toSqlQuery() {
    return this.fieldName + " IS NULL";
  }
}
