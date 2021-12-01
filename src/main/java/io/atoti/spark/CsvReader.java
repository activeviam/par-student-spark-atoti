package io.atoti.spark;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Objects;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CsvReader {
    public static Dataset<Row> read(String path, SparkSession session) throws URISyntaxException {
        final ClassLoader cl = Thread.currentThread().getContextClassLoader();
        final URL url = Objects.requireNonNull(cl.getResource(path), "Cannot find file");

        final Dataset<Row> dataframe =
            session
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", true)
                .option("dateFormat", "dd/MM/yyyy")
                .option("inferSchema", true)
                .load(url.toURI().getPath());

        return dataframe;
    }
}
