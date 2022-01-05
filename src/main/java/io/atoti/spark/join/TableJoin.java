package io.atoti.spark.join;

import io.atoti.spark.Queryable;
import java.util.Set;

public record TableJoin(
    String name, String sourceTableName, String targetTableName, Set<FieldMapping> fieldMappings)
    implements Queryable {
  public String toSqlQuery() {
    final String joinConditions =
        String.join(
            "\nAND ",
            fieldMappings.stream()
                .map(
                    (fieldMapping) ->
                        sourceTableName
                            + "."
                            + fieldMapping.sourceField()
                            + " = "
                            + targetTableName
                            + "."
                            + fieldMapping.targetField())
                .toList());
    return sourceTableName + " JOIN " + targetTableName + " ON " + joinConditions;
  }
}
