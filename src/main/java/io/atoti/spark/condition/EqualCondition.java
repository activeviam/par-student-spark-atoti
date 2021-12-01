package io.atoti.spark.condition;

public record EqualCondition(String column, Object value) implements QueryCondition {

}
