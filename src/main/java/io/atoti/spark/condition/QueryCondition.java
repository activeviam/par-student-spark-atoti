package io.atoti.spark.condition;

import java.io.Serializable;
import org.apache.spark.sql.Column;

public sealed interface QueryCondition extends Serializable
    permits AndCondition,
        OrCondition,
        NotCondition,
        EqualCondition,
        NullCondition,
        TrueCondition,
        FalseCondition {
  Column getCondition();

  String toSqlQuery();
}
