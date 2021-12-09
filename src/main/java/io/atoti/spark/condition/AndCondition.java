package io.atoti.spark.condition;

import java.util.List;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;

public record AndCondition(List<QueryCondition> conditions) implements QueryCondition {

  public AndCondition {
    if (conditions.isEmpty()) {
      throw new IllegalArgumentException("Cannot accept an empty list of conditions");
    }
  }

  public static AndCondition of(final QueryCondition... conditions) {
    return new AndCondition(List.of(conditions));
  }

  @Override
  public FilterFunction<Row> getCondition() {
    return (Row row) ->
        this.conditions.stream()
            .allMatch(
                (condition) -> {
                  final var filterFunction = condition.getCondition();
                  try {
                    return filterFunction.call(row);
                  } catch (Exception e) {
                    throw new IllegalStateException("Failed to execute condition " + condition, e);
                  }
                });
  }

  @Override
  public String toSqlQuery() {
	var conditionsSql = this.conditions.stream()
	        .map((condition) -> "(" + condition.toSqlQuery() + ")").toArray(String[]::new);
    return String.join(" AND ", conditionsSql);
  }
}
