package io.atoti.spark.condition;

import java.util.List;

public record AndCondition(List<QueryCondition> conditions) implements QueryCondition {

  public AndCondition {
    if (conditions.isEmpty()) {
      throw new IllegalArgumentException("Cannot accept an empty list of conditions");
    }
  }

  public static AndCondition of(final QueryCondition... conditions) {
    return new AndCondition(List.of(conditions));
  }
}
