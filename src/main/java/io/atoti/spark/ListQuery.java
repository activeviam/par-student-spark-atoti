package io.atoti.spark;

import static org.apache.spark.sql.functions.monotonically_increasing_id;

import io.atoti.spark.condition.QueryCondition;
import java.util.List;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

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

  public static List<Row> list(Dataset<Row> dataframe, List<String> wantedColumns, QueryCondition condition, int limit, int offset) {
    if (offset < 0) {
      throw new IllegalArgumentException("Cannot accept a negative offset");
    }

    final Column[] columns = wantedColumns.stream().map(functions::col).toArray(Column[]::new);
    dataframe = dataframe.filter(condition.getCondition()).withColumn("_id", monotonically_increasing_id());

    if (limit < 0) {
      dataframe = dataframe.where(dataframe.col("_id").geq(offset));
    } else {
      dataframe = dataframe.where(dataframe.col("_id").between(offset, offset + limit - 1));
    }

    return dataframe.select(columns).collectAsList();
  }

  public static List<Row> listSql(
      SparkSession spark, String table, List<String> wantedColumns, int limit, int offset) {
    String wantedColumnsStatement =
        wantedColumns.isEmpty() ? "*" : String.join(", ", wantedColumns);
    String limitStatement = limit >= 0 ? " LIMIT " + limit + " " : "";
    String tableStatement =
        offset > 0
            ? "(SELECT * FROM "
                + table
                + " WHERE monotonically_increasing_id() >= "
                + offset
                + " LIMIT "
                + limit
                + ")"
            : table;
    return spark
        .sql("SELECT " + wantedColumnsStatement + " FROM " + tableStatement + limitStatement + ";")
        .collectAsList();
  }

  public static List<Row> listSql(SparkSession spark, String table, QueryCondition condition) {
    return spark
        .sql("SELECT * FROM " + table + " WHERE " + condition.toSqlQuery() + ";")
        .collectAsList();
  }
}
