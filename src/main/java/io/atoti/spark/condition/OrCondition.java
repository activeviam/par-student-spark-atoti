package io.atoti.spark.condition;

import java.util.List;
import org.apache.spark.sql.Column;

public record OrCondition(List<QueryCondition> conditions) implements QueryCondition {

  public OrCondition {
    if (conditions.isEmpty()) {
      throw new IllegalArgumentException("Cannot accept an empty list of conditions");
    }
  }

  public static OrCondition of(final QueryCondition... conditions) {
    return new OrCondition(List.of(conditions));
  }

  @Override
  public Column getCondition() {
    return this.conditions.stream()
        .map(QueryCondition::getCondition)
        .reduce(FalseCondition.value().getCondition(), (Column a, Column b) -> a.$bar$bar(b));
  }

  @Override
  public String toSqlQuery() {
    return String.join(
        " OR ",
        this.conditions.stream().map((condition) -> "(" + condition.toSqlQuery() + ")").toList());
  }
}
