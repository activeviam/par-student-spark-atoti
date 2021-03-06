package io.atoti.spark.operation;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.element_at;

import io.atoti.spark.aggregation.AggregatedValue;
import java.util.List;

public final class VectorAt extends Operation {

  public VectorAt(String name, String arrayColumn, int position) {
    super(name);
    this.column = element_at(col(arrayColumn), position).alias(name);
  }

  public VectorAt(String name, AggregatedValue arrayColumn, int position) {
    super(name);
    this.column = element_at(arrayColumn.toColumn(), position).alias(name);
    this.neededAggregations = List.of(arrayColumn);
  }

  public VectorAt(String name, Operation arrayColumn, int position) {
    super(name);
    this.column = element_at(arrayColumn.toColumn(), position).alias(name);
    this.neededOperations = List.of(arrayColumn);
  }
}
