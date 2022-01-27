package io.atoti.spark;

import static org.apache.spark.sql.functions.monotonically_increasing_id;

import io.atoti.spark.condition.QueryCondition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;

public class ListQuery {

  /**
   * Retrieves a series of rows from a dataframe.
   *
   * @param wantedColumns names of the columns we want to see in the resulting list
   * @param limit max number of rows to return. Passing a negative number disable this option
   * @param offset offset at which this starts reading the input dataframe
   * @return list of rows extracted from the DataFrame
   */
  public static List<Row> list(
      Dataset<Row> dataframe, List<String> wantedColumns, int limit, int offset) {
    if (offset < 0) {
      throw new IllegalArgumentException("Cannot accept a negative offset");
    }

    final Column[] columns = wantedColumns.stream().map(functions::col).toArray(Column[]::new);
    dataframe = dataframe.withColumn("_id", monotonically_increasing_id());

    if (limit < 0) {
      return dataframe.where(dataframe.col("_id").geq(offset)).select(columns).collectAsList();
    } else {
      return dataframe
          .where(dataframe.col("_id").between(offset, offset + limit - 1))
          .select(columns)
          .collectAsList();
    }
  }

  public static List<Row> list(Dataset<Row> dataframe, QueryCondition condition) {
    return dataframe.filter(condition.getCondition()).collectAsList();
  }

  public static List<Row> list(
      Dataset<Row> dataframe,
      List<String> wantedColumns,
      QueryCondition condition,
      int limit,
      int offset) {
    if (offset < 0) {
      throw new IllegalArgumentException("Cannot accept a negative offset");
    }

    final Column[] columns = wantedColumns.stream().map(functions::col).toArray(Column[]::new);
    dataframe =
        zipWithIndex(dataframe.filter(condition.getCondition()));

    if (limit < 0) {
      dataframe = dataframe.where(dataframe.col("_id").geq(offset));
    } else {
      dataframe = dataframe.where(dataframe.col("_id").between(offset, offset + limit - 1));
    }

    return dataframe.select(columns).collectAsList();
  }

  public static List<Row> listSql(
    SparkSession spark, Queryable table, List<String> wantedColumns, int limit, int offset) {
  String wantedColumnsStatement =
      wantedColumns.isEmpty() ? "*" : String.join(", ", wantedColumns);
  String limitStatement = limit >= 0 ? " LIMIT " + limit + " " : "";
  String tableStatement =
      offset > 0
          ? "(SELECT * FROM "
              + table.toSqlQuery()
              + " WHERE monotonically_increasing_id() >= "
              + offset
              + ")"
          : table.toSqlQuery();
  return spark
      .sql("SELECT " + wantedColumnsStatement + " FROM " + tableStatement + limitStatement + ";")
      .collectAsList();
}

  public static List<Row> listSql(SparkSession spark, Queryable table, QueryCondition condition) {
    return spark
        .sql("SELECT * FROM " + table.toSqlQuery() + " WHERE " + condition.toSqlQuery() + ";")
        .collectAsList();
  }
  
  private static Dataset<Row> zipWithIndex(Dataset<Row> df) {
      Dataset<Row> dfWithPartitionId = df
              .withColumn("partition_id", functions.spark_partition_id())
              .withColumn("inc_id", monotonically_increasing_id());

      Object partitionOffsetsObject = dfWithPartitionId
              .groupBy("partition_id")
              .agg(functions.count(functions.lit(1)).alias("cnt"), functions.first("inc_id").alias("inc_id"))
              .orderBy("partition_id")
              .select(functions.col("partition_id"), functions.sum("cnt").over(Window.orderBy("partition_id")).minus(functions.col("cnt")).minus(functions.col("inc_id")).alias("cnt"))
              .collect();
      Row[] partitionOffsetsArray = ((Row[]) partitionOffsetsObject);
      Map<Integer, Long> partitionOffsets = new HashMap<Integer, Long>();
      for (int i = 0; i < partitionOffsetsArray.length; i++) {
          partitionOffsets.put(partitionOffsetsArray[i].getInt(0), partitionOffsetsArray[i].getLong(1));
      }

      UserDefinedFunction getPartitionOffset = functions.udf(
              (partitionId) -> partitionOffsets.get((Integer) partitionId), DataTypes.LongType
      );

      return dfWithPartitionId
              .withColumn("partition_offset", getPartitionOffset.apply(functions.col("partition_id")))
              .withColumn("_id", functions.col("inc_id").plus(functions.col("partition_offset")))
              .drop("partition_id", "partition_offset", "inc_id");
  }
}
