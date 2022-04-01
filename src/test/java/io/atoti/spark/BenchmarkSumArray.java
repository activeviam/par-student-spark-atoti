package io.atoti.spark;

import io.atoti.spark.aggregation.*;
import io.atoti.spark.condition.EqualCondition;
import io.atoti.spark.condition.QueryCondition;
import io.github.cdimascio.dotenv.Dotenv;
import org.apache.spark.sql.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(
    value = 1,
    jvmArgs = {"--enable-preview", "--illegal-access=permit"})
public class BenchmarkSumArray {
    SparkSession spark;
    Dataset<Row> dataframe;
    String tableName;

  public static void main(String[] args) throws Exception {
    Options opt = new OptionsBuilder().include(BenchmarkSumArray.class.getSimpleName()).build();
    new Runner(opt).run();
  }

  @Setup()
  public void setup() {
    Dotenv dotenv = Dotenv.load();
    spark = SparkSession.builder()
            .appName("Spark Atoti")
            .config("spark.master", "local")
            .config("spark.databricks.service.clusterId", dotenv.get("clusterId"))
            .getOrCreate();
    spark.sparkContext().addJar("./target/spark-lib-0.0.1-SNAPSHOT.jar");
    spark.sparkContext().setLogLevel("ERROR");
    dataframe = spark.read().table("table_vector_100");
    tableName = "table_vector_100";
  }

  @Benchmark
  @BenchmarkMode(Mode.SingleShotTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Warmup(iterations = 3)
  @Measurement(iterations = 10)
  public void benchmarkSparkApiSumArray(Blackhole bh) {
    final Dataset<Row> rows = AggregateQuery.aggregate(dataframe, List.of("category"), List.of(new SumArray("sum", "price_simulations")));
    rows.show(); // mandatory to trigger the computation of the dataset
    bh.consume(rows);
  }

  @Benchmark
  @BenchmarkMode(Mode.SingleShotTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Warmup(iterations = 3)
  @Measurement(iterations = 10)
  public void benchmarkSparkSqlSumArray(Blackhole bh) {
    final Dataset<Row> rows = spark.sql("SELECT category, collect_list(sum) FROM (SELECT category, sum(col) as sum FROM (SELECT category, posexplode(price_simulations) FROM table_vector_100) GROUP BY category, pos ORDER BY pos) GROUP BY category;");
    rows.show(); // mandatory to trigger the computation of the dataset
    bh.consume(rows);
  }
}
