package io.atoti.spark.operation;

import static io.atoti.spark.Utils.convertScalaArrayToArray;
import static io.atoti.spark.Utils.convertToArrayListToScalaArraySeq;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;

import io.atoti.spark.aggregation.AggregatedValue;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.immutable.ArraySeq;

public final class Multiply extends Operation {

  private static UserDefinedFunction udf =
      udf(
          (Long x, ArraySeq<Long> s) -> {
            ArrayList<Long> list = convertScalaArrayToArray(s);
            return convertToArrayListToScalaArraySeq(
                list.stream().map((Long value) -> value * x).toList());
          },
          DataTypes.createArrayType(DataTypes.LongType));

  public Multiply(String name, String scalarColumn, String arrayColumn) {
    super(name);
    this.column = Multiply.udf.apply(col(scalarColumn), col(arrayColumn)).alias(name);
  }

  public Multiply(String name, String scalarColumn, AggregatedValue arrayColumn) {
    super(name);
    this.column = Multiply.udf.apply(col(scalarColumn), arrayColumn.toColumn()).alias(name);
    this.neededAggregations = List.of(arrayColumn);
  }

  public Multiply(String name, String scalarColumn, Operation arrayColumn) {
    super(name);
    this.column = Multiply.udf.apply(col(scalarColumn), arrayColumn.toColumn()).alias(name);
    this.neededOperations = List.of(arrayColumn);
  }

  public Multiply(String name, AggregatedValue scalarColumn, String arrayColumn) {
    super(name);
    this.column = Multiply.udf.apply(scalarColumn.toColumn(), col(arrayColumn)).alias(name);
    this.neededAggregations = List.of(scalarColumn);
  }

  public Multiply(String name, AggregatedValue scalarColumn, AggregatedValue arrayColumn) {
    super(name);
    this.column = Multiply.udf.apply(scalarColumn.toColumn(), arrayColumn.toColumn()).alias(name);
    this.neededAggregations = List.of(scalarColumn, arrayColumn);
  }

  public Multiply(String name, AggregatedValue scalarColumn, Operation arrayColumn) {
    super(name);
    this.column = Multiply.udf.apply(scalarColumn.toColumn(), arrayColumn.toColumn()).alias(name);
    this.neededAggregations = List.of(scalarColumn);
    this.neededOperations = List.of(arrayColumn);
  }

  public Multiply(String name, Operation scalarColumn, String arrayColumn) {
    super(name);
    this.column = Multiply.udf.apply(scalarColumn.toColumn(), col(arrayColumn)).alias(name);
    this.neededOperations = List.of(scalarColumn);
  }

  public Multiply(String name, Operation scalarColumn, AggregatedValue arrayColumn) {
    super(name);
    this.column = Multiply.udf.apply(scalarColumn.toColumn(), arrayColumn.toColumn()).alias(name);
    this.neededAggregations = List.of(arrayColumn);
    this.neededOperations = List.of(scalarColumn);
  }

  public Multiply(String name, Operation scalarColumn, Operation arrayColumn) {
    super(name);
    this.column = Multiply.udf.apply(scalarColumn.toColumn(), arrayColumn.toColumn()).alias(name);
    this.neededOperations = List.of(scalarColumn, arrayColumn);
  }

  public Column toAggregateColumn() {
    return this.column;
  }
}
