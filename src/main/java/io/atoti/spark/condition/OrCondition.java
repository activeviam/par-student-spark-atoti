package io.atoti.spark.condition;

import static java.util.stream.Collectors.toList;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.Column;

public class OrCondition implements QueryCondition {
  private List<QueryCondition> conditions;

  public OrCondition(List<QueryCondition> conditions) {
    if (conditions.isEmpty()) {
      throw new IllegalArgumentException("Cannot accept an empty list of conditions");
    }
    this.conditions = conditions;
  }

  public static OrCondition of(final QueryCondition... conditions) {
    return new OrCondition(Arrays.asList(conditions));
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
        this.conditions.stream().map((condition) -> "(" + condition.toSqlQuery() + ")").collect(toList()));
  }
}
