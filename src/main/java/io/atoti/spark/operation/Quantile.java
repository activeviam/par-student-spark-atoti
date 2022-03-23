package io.atoti.spark.operation;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;

import io.atoti.spark.Utils;
import io.atoti.spark.aggregation.AggregatedValue;

import java.util.ArrayList;
import java.util.Arrays;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.JavaConverters;
import scala.collection.Seq;

public final class Quantile extends Operation {

  static UserDefinedFunction quantileUdf(float percent) {
    return udf(
        (Seq<Long> arr) -> {
          ArrayList<Long> javaArr = new ArrayList<Long>(JavaConverters.asJavaCollectionConverter(arr).asJavaCollection());
          return Utils.quantile(javaArr, percent);
        },
        DataTypes.LongType);
  }

  public Quantile(String name, String arrayColumn, float percent) {
    super(name);
    this.column = quantileUdf(percent).apply(col(arrayColumn)).alias(name);
  }

  public Quantile(String name, AggregatedValue arrayColumn, float percent) {
    super(name);
    this.column = quantileUdf(percent).apply(arrayColumn.toColumn()).alias(name);
    this.neededAggregations = Arrays.asList(arrayColumn);
  }

  public Quantile(String name, Operation arrayColumn, float percent) {
    super(name);
    this.column = quantileUdf(percent).apply(arrayColumn.toColumn()).alias(name);
    this.neededOperations = Arrays.asList(arrayColumn);
  }
}
