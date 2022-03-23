package io.atoti.spark.operation;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;

import io.atoti.spark.Utils;
import io.atoti.spark.aggregation.AggregatedValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.compat.immutable.ArraySeq;

public final class QuantileIndex extends Operation {

  static UserDefinedFunction quantileIndexUdf(float percent) {
    return udf(
        (ArraySeq<Long> arr) -> {
          ArrayList<Long> javaArr = Utils.convertScalaArrayToArray(arr);
          return Utils.quantileIndex(javaArr, percent) + 1;
        },
        DataTypes.IntegerType);
  }

  public QuantileIndex(String name, String arrayColumn, float percent) {
    super(name);
    this.column = quantileIndexUdf(percent).apply(col(arrayColumn)).alias(name);
  }

  public QuantileIndex(String name, AggregatedValue arrayColumn, float percent) {
    super(name);
    this.column = quantileIndexUdf(percent).apply(arrayColumn.toColumn()).alias(name);
    this.neededAggregations = Arrays.asList(arrayColumn);
  }

  public QuantileIndex(String name, Operation arrayColumn, float percent) {
    super(name);
    this.column = quantileIndexUdf(percent).apply(arrayColumn.toColumn()).alias(name);
    this.neededOperations = Arrays.asList(arrayColumn);
  }
}
