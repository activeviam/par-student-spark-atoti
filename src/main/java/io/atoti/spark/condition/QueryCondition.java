package io.atoti.spark.condition;

import java.io.Serializable;
import org.apache.spark.sql.Column;

public interface QueryCondition extends Serializable {
  Column getCondition();

  String toSqlQuery();
}
