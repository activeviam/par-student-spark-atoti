package io.atoti.spark.join;

import io.atoti.spark.Queryable;
import java.util.Set;

import static java.util.stream.Collectors.toList;

public class TableJoin implements Queryable {
  Queryable sourceTable;
  String targetTableName;
  Set<FieldMapping> fieldMappings;

  public TableJoin(Queryable sourceTable, String targetTableName, Set<FieldMapping> fieldMappings) {
    this.sourceTable = sourceTable;
    this.targetTableName = targetTableName;
    this.fieldMappings = fieldMappings;
  }

  public String toSqlQuery() {
    final String joinConditions =
        String.join(
            "\nAND ",
            fieldMappings.stream()
                .map(
                    (fieldMapping) ->
                        fieldMapping.getSourceField()
                            + " = "
                            + fieldMapping.getTargetField()
                            + " OR ("
                            + fieldMapping.getSourceField()
                            + " IS NULL AND "
                            + fieldMapping.getTargetField()
                            + " IS NULL)")
                .collect(toList()));
    return sourceTable.toSqlQuery() + "\nJOIN " + targetTableName + " ON " + joinConditions;
  }
}
