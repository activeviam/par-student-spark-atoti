package io.atoti.spark;

import io.atoti.spark.aggregation.AggregatedValue;
import io.atoti.spark.condition.QueryCondition;
import io.atoti.spark.condition.TrueCondition;
import io.atoti.spark.operation.Operation;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class AggregateQuery {

  /**
   * Retrieves the aggregations of rows from a dataframe.
   *
   * @param groupByColumns names of the columns to consider to group-by the provided dataframe
   * @param aggregations aggregated values to return with the result
   */
  public static Dataset<Row> aggregate(
      Dataset<Row> dataframe, List<String> groupByColumns, List<AggregatedValue> aggregations) {
    return aggregate(dataframe, groupByColumns, aggregations, TrueCondition.value());
  }

  public static Dataset<Row> aggregate(
      Dataset<Row> dataframe,
      List<String> groupByColumns,
      List<AggregatedValue> aggregations,
      QueryCondition condition) {
    return aggregate(dataframe, groupByColumns, aggregations, List.of(), condition);
  }
  
  public static Dataset<Row> aggregate(
	      Dataset<Row> dataframe,
	      List<String> groupByColumns,
	      List<AggregatedValue> aggregations,
	      List<Operation> operations) {
	  return aggregate(dataframe, groupByColumns, aggregations, operations, TrueCondition.value());
  }
  
  public static Dataset<Row> aggregate(
	      Dataset<Row> dataframe,
	      List<String> groupByColumns,
	      List<AggregatedValue> aggregations,
	      List<Operation> operations,
	      QueryCondition condition) {
	  if (aggregations.isEmpty() && operations.isEmpty()) {
	      throw new IllegalArgumentException(
	          "#aggregate can only be called with at least one AggregatedValue or Operation");
	    }

	    final Column[] columns = groupByColumns.stream().map(functions::col).toArray(Column[]::new);
	    final Column[] createdAggregatedColumns =
		        aggregations.stream().map(AggregatedValue::toColumn).toArray(Column[]::new);
	    final Column[] createdOperationColumns =
		        operations.stream().map(Operation::toColumn).toArray(Column[]::new);
	    final Column[] columnsToSelect = Arrays.copyOf(columns, columns.length + createdAggregatedColumns.length + createdOperationColumns.length);
	    System.arraycopy(createdAggregatedColumns, 0, columnsToSelect, columns.length, createdAggregatedColumns.length);
	    System.arraycopy(createdOperationColumns, 0, columnsToSelect, columns.length + createdAggregatedColumns.length, createdOperationColumns.length);
	    
		// Add needed aggregations for operations to the `aggregations` list
	    final List<AggregatedValue> neededAggregations = Stream.concat(operations.stream().flatMap((Operation op) -> op.getNeededAggregations()), aggregations.stream()).distinct().toList();
	    		
	    // Aggregations
	    if (!neededAggregations.isEmpty()) {
		    final Column firstAggColumn = neededAggregations.get(0).toAggregateColumn();
		    final Column[] nextAggColumns =
		    	neededAggregations.subList(1, neededAggregations.size()).stream()
		            .map(AggregatedValue::toAggregateColumn)
		            .toArray(Column[]::new);
		    dataframe = dataframe
		        .filter(condition.getCondition())
		        .groupBy(columns)
		        .agg(firstAggColumn, nextAggColumns);
	    }
	    
	    // Operations
	    if (!operations.isEmpty()) {
		  for (Operation op : operations.stream().flatMap(Operation::getAllOperations).toList()) {
			  dataframe = dataframe.withColumn(op.getName(), op.toAggregateColumn());
		  }
	    }
	    
	    
	    return dataframe.select(columnsToSelect);
  }

  public static Dataset<Row> aggregateSql(
      SparkSession spark,
      String table,
      List<String> groupByColumns,
      List<AggregatedValue> aggregations) {
    return aggregateSql(spark, table, groupByColumns, aggregations, TrueCondition.value());
  }

  public static Dataset<Row> aggregateSql(
      SparkSession spark,
      String table,
      List<String> groupByColumns,
      List<AggregatedValue> aggregations,
      QueryCondition condition) {
    if (aggregations.isEmpty()) {
      throw new IllegalArgumentException(
          "#aggregate can only be called with at least one AggregatedValue");
    }
    String[] aggregationsSql =
        aggregations.stream().map(AggregatedValue::toSqlQuery).toArray(String[]::new);
    String query =
        "SELECT "
            + String.join(", ", groupByColumns)
            + ", "
            + String.join(", ", aggregationsSql)
            + " FROM "
            + table
            + " WHERE "
            + condition.toSqlQuery()
            + " GROUP BY "
            + String.join(", ", groupByColumns)
            + ";";
    return spark.sql(query);
  }
}
