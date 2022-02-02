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

@State(Scope.Benchmark)
@Fork(
    value = 1,
    jvmArgs = {"--enable-preview", "--illegal-access=permit"})
public class BenchmarkSparkSql {
    SparkSession spark;
    Dataset<Row> dataframe;
    String tableName;
    int limit;
    int offset;
    List<String> wantedColumns;
    QueryCondition condition;
    List<String> groupByColumns;
    List<AggregatedValue> aggregation;

  public static void main(String[] args) throws Exception {
    Options opt = new OptionsBuilder().include(BenchmarkSparkSql.class.getSimpleName()).build();
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
        dataframe = spark.read().table("us_accidents_15m");
        tableName = "us_accidents_15m";
        limit = 100000;
        offset = 100000;
        wantedColumns = List.of("ID", "Severity");
        condition = new EqualCondition("Severity", 4);
        groupByColumns = List.of("Severity");
        aggregation = List.of(new Count("severity_count"));
    }

  @Benchmark
  @BenchmarkMode(Mode.SingleShotTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Warmup(iterations = 3)
  @Measurement(iterations = 10)
  public void benchmarkSparkApiListLimit(Blackhole bh) {
    final List<Row> rows = ListQuery.list(dataframe, wantedColumns, limit, offset);
    bh.consume(rows);
  }

  @Benchmark
  @BenchmarkMode(Mode.SingleShotTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Warmup(iterations = 3)
  @Measurement(iterations = 10)
  public void benchmarkSparkSqlListLimit(Blackhole bh) {
    final List<Row> rows =
        ListQuery.listSql(spark, new Table(tableName), wantedColumns, limit, offset);
    bh.consume(rows);
  }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 3)
    @Measurement(iterations = 10)
    public void benchmarkSparkApiListCondition(Blackhole bh) {
        final List<Row> rows = ListQuery.list(dataframe, condition);
        bh.consume(rows);
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 3)
    @Measurement(iterations = 10)
    public void benchmarkSparkSqlListCondition(Blackhole bh) {
        final List<Row> rows = ListQuery.listSql(spark, tableName, condition);
        bh.consume(rows);
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 3)
    @Measurement(iterations = 10)
    public void benchmarkSparkApiAggregation(Blackhole bh) {
        final Dataset<Row> rows = AggregateQuery.aggregate(dataframe, groupByColumns, aggregation);
        rows.show(); // mandatory to trigger the computation of the dataset
        bh.consume(rows);
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 3)
    @Measurement(iterations = 10)
    public void benchmarkSparkSqlAggregation(Blackhole bh) {
        final Dataset<Row> rows = AggregateQuery.aggregateSql(spark, tableName, groupByColumns, aggregation);
        rows.show(); // mandatory to trigger the computation of the dataset
        bh.consume(rows);
    }
}
