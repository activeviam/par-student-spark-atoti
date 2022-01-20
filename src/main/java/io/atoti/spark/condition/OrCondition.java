package io.atoti.spark.condition;

import java.util.List;
import org.apache.spark.sql.Column;

public class OrCondition implements QueryCondition {
  List<QueryCondition> conditions;

  public OrCondition(List<QueryCondition> conditions) {
    if (conditions.isEmpty()) {
      throw new IllegalArgumentException("Cannot accept an empty list of conditions");
    }
    this.conditions = conditions;
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
