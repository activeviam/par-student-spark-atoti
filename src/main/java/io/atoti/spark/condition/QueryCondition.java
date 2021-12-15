package io.atoti.spark.condition;

import java.io.Serializable;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;

public sealed interface QueryCondition extends Serializable
    permits AndCondition,
        OrCondition,
        NotCondition,
        EqualCondition,
        NullCondition,
        TrueCondition,
        FalseCondition {
  FilterFunction<Row> getCondition();

  String toSqlQuery();
}
