package io.atoti.spark.join;

import io.atoti.spark.Queryable;
import java.util.Set;

public record TableJoin(
    Queryable sourceTable, String targetTableName, Set<FieldMapping> fieldMappings)
    implements Queryable {
  public String toSqlQuery() {
    final String joinConditions =
        String.join(
            "\nAND ",
            fieldMappings.stream()
                .map(
                    (fieldMapping) ->
                        fieldMapping.sourceField()
                            + " = "
                            + fieldMapping.targetField()
                            + " OR ("
                            + fieldMapping.sourceField()
                            + " IS NULL AND "
                            + fieldMapping.targetField()
                            + " IS NULL)")
                .toList());
    return sourceTable.toSqlQuery() + "\nJOIN " + targetTableName + " ON " + joinConditions;
  }
}
