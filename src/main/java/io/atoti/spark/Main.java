package io.atoti.spark;

import io.atoti.spark.aggregation.Sum;
import io.atoti.spark.aggregation.SumArray;
import io.atoti.spark.operation.Multiply;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {

  public static void main(String[] args) {
    final SparkSession spark =
        SparkSession.builder().appName("Spark Atoti").config("spark.master", "local").getOrCreate();
    Dataset<Row> dataframe = CsvReader.read("csv/array.csv", spark);
    var t =
        AggregateQuery.aggregate(
            dataframe,
            List.of("id"),
            List.of(
                new Sum("f", "quantity"),
                new SumArray(
                    "vector", "price_simulations", spark.implicits().newLongArrayEncoder())),
            List.of(
                new Multiply(
                    "f * vector",
                    new Sum("sum(quantity)", "quantity"),
                    new SumArray(
                        "sum(price_simulations)",
                        "price_simulations",
                        spark.implicits().newLongArrayEncoder())),
                new Multiply(
                    "f * f * vector",
                    new Sum("sum(quantity)", "quantity"),
                    new Multiply(
                        "f * vector",
                        new Sum("sum(quantity)", "quantity"),
                        new SumArray(
                            "sum(price_simulations)",
                            "price_simulations",
                            spark.implicits().newLongArrayEncoder())))));

    t.show();
  }
}
