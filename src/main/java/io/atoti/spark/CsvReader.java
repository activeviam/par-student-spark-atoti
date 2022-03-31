package io.atoti.spark;

import java.net.URISyntaxException;
import java.net.URL;
import java.util.Objects;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

public class CsvReader {

  public static Dataset<Row> read(String path, SparkSession session) {
    return CsvReader.read(path, session, ";");
  }

  public static Dataset<Row> read(String path, SparkSession session, String separator) {
    final ClassLoader cl = Thread.currentThread().getContextClassLoader();
    final URL url = Objects.requireNonNull(cl.getResource(path), "Cannot find file");

    Dataset<Row> dataframe;
    try {
      dataframe =
          session
              .read()
              .format("csv")
              .option("sep", separator)
              .option("header", true)
              .option("timestampFormat", "dd/MM/yyyy")
              .option("inferSchema", true)
              .load(url.toURI().getPath());
      for (int i = 0; i < dataframe.columns().length; i++) {
        if (dataframe.dtypes()[i]._2 == DataTypes.StringType.toString()) {
          String col = dataframe.columns()[i];
          if (dataframe
                  .filter(functions.col(col).$eq$eq$eq("").$bar$bar(functions.col(col).rlike(",")))
                  .count()
              == dataframe.count()) {
            // This prototype only supports arrays of integers
            dataframe =
                dataframe.withColumn(
                    col, functions.split(functions.col(col), ",").cast("array<long>"));
          }
        }
      }
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Failed to read csv " + path, e);
    }

    return dataframe;
  }
}
