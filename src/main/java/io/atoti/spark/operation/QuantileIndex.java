package io.atoti.spark.operation;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;

import io.atoti.spark.Utils;
import io.atoti.spark.aggregation.AggregatedValue;

import java.util.ArrayList;
import java.util.Arrays;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import scala.Serializable;
import scala.collection.Seq;

public final class QuantileIndex extends Operation {

  static UserDefinedFunction quantileIndexUdf(float percent) {
    return udf(
        new QuantileUdf(percent),
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

  private static class QuantileUdf implements UDF1<Seq<Long>, Integer>, Serializable {
    private static final long serialVersionUID = 20220330_1217L;

    private final float percent;

    public QuantileUdf(float percent) {
      this.percent = percent;
    }

    @Override
    public Integer call(Seq<Long> arr) {
      ArrayList<Long> javaArr = Utils.convertScalaArrayToArray(arr);
      return Utils.quantileIndex(javaArr, percent) + 1;
    }
  }
}
