package io.atoti.spark.condition;

import java.util.List;
import org.apache.spark.sql.Column;

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
  public Column getCondition() {
    return this.conditions.stream()
        .map(QueryCondition::getCondition)
        .reduce(TrueCondition.value().getCondition(), (Column a, Column b) -> a.$amp$amp(b));
  }

  @Override
  public String toSqlQuery() {
    return String.join(
        " AND ",
        this.conditions.stream().map((condition) -> "(" + condition.toSqlQuery() + ")").toList());
  }
}
