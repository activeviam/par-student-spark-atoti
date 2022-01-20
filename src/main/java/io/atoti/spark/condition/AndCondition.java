package io.atoti.spark.condition;

import java.util.List;
import org.apache.spark.sql.Column;

public class AndCondition implements QueryCondition {
  List<QueryCondition> conditions;

  public AndCondition(List<QueryCondition> conditions) {
    if (conditions.isEmpty()) {
      throw new IllegalArgumentException("Cannot accept an empty list of conditions");
    }
    this.conditions = conditions;
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
