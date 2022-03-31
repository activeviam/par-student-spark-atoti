package io.atoti.spark.join;

public class FieldMapping {
  String sourceField;
  String targetField;

  public FieldMapping(String sourceField, String targetField) {
    this.sourceField = sourceField;
    this.targetField = targetField;
  }

  public String getSourceField() {
    return sourceField;
  }

  public String getTargetField() {
    return targetField;
  }
}
