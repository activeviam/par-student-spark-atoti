package io.atoti.spark.condition;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

public class EqualCondition implements QueryCondition {
  String column;
  Object value;

  public EqualCondition(String column, Object value) {
    this.column = column;
    this.value = value;
  }

  @Override
  public Column getCondition() {
    return functions.col(column).$eq$eq$eq(value);
  }

  @Override
  public String toSqlQuery() {
    return this.column + " = \"" + this.value + "\"";
  }
}
