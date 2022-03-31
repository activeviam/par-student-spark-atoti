/*
 * (C) ActiveViam 2021
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package io.atoti.spark.condition;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

public class FalseCondition implements QueryCondition {

  public static FalseCondition value() {
    return new FalseCondition();
  }

  @Override
  public Column getCondition() {
    return functions.lit(false);
  }

  @Override
  public String toSqlQuery() {
    return "false";
  }
}
