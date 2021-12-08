package io.atoti.spark.condition;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;

public record EqualCondition(String column, Object value) implements QueryCondition {

  @Override
  public FilterFunction<Row> getCondition() {
    return (Row row) -> row.getAs(this.column).equals(this.value);
  }
}
