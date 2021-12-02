/*
 * (C) ActiveViam 2021
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package io.atoti.spark.condition;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;

public record FalseCondition() implements QueryCondition {

  public static FalseCondition value() {
    return new FalseCondition();
  }

  @Override
  public FilterFunction<Row> getCondition() {
    return (Row row) -> false;
  }
}
