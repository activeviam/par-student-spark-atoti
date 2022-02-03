package io.atoti.spark.aggregation;

import static io.atoti.spark.Utils.convertScalaArrayToArray;
import static io.atoti.spark.Utils.convertToArrayListToScalaArraySeq;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;

import java.util.ArrayList;
import java.util.Objects;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

import scala.collection.immutable.ArraySeq;

public record Multiply(String name, AggregatedValue lhs, AggregatedValue rhs, Dataset<Row> dataframe)
    implements AggregatedValue {

  public Multiply {
    Objects.requireNonNull(name, "No name provided");
    Objects.requireNonNull(lhs, "No left-hand value provided");
    Objects.requireNonNull(rhs, "No right-hand value provided");
  }
  
  private static UserDefinedFunction udf = udf((ArraySeq<Integer> s, Integer x) -> {
	  ArrayList<Integer> list = convertScalaArrayToArray(s);
	  return convertToArrayListToScalaArraySeq(new ArrayList<Integer>(list.stream().map((Integer value) -> value * x).toList()));
  }, DataTypes.createArrayType(DataTypes.IntegerType));

  public Column toAggregateColumn() {
	  return Multiply.udf.apply(lhs.toAggregateColumn(), rhs.toAggregateColumn()).alias(name);
  }

  public Column toColumn() {
    return col(name);
  }

  public String toSqlQuery() {
    throw new UnsupportedOperationException("TODO");
  }
}
