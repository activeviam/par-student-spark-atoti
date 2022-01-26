package io.atoti.spark.condition;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

public record EqualCondition(String column, Object value) implements QueryCondition {

  @Override
  public Column getCondition() {
    return functions.col(column).$eq$eq$eq(value);
  }

  @Override
  public String toSqlQuery() {
    return this.column + " = \"" + this.value + "\"";
  }
}
