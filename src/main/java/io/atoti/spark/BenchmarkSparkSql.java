package io.atoti.spark;

import org.apache.spark.sql.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value=1, jvmArgs={"--enable-preview", "--illegal-access=permit"})
public class BenchmarkSparkSql {
    SparkSession spark;
    Dataset<Row> dataframe;
    String tableName;
    int limit;
    int offset;
    List<String> wantedColumns;

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(BenchmarkSparkSql.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Setup()
    public void setup() {
        spark = SparkSession.builder().appName("Spark Atoti").config("spark.master", "local").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        dataframe = CsvReader.read("csv/US_accidents_Dec20_updated.csv", spark, ",");
        tableName = "my_table";
        limit = 100000;
        offset = 100000;
        wantedColumns = List.of("ID", "Severity");
        dataframe.createOrReplaceTempView(tableName);
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 3)
    @Measurement(iterations = 10)
    public void benchmarkSparkApi(Blackhole bh) {
        final List<Row> rows = ListQuery.list(dataframe, wantedColumns, limit, offset);
        bh.consume(rows);
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 3)
    @Measurement(iterations = 10)
    public void benchmarkSparkSql(Blackhole bh) {
        final List<Row> rows = ListQuery.listSql(spark, tableName, wantedColumns, limit, offset);
        bh.consume(rows);
    }
}

