# par-student-spark-atoti

Project with students from Centrale-Supélec to explore Spark API in order to power atoti with Spark

# Status

This project aims to explore Spark capabilities. In this project, we will experiment various methods
but it will remain a prototype.

This does not imply any future feature of Atoti+.

# Running the project

This project is based on [Maven wrapper](https://github.com/takari/maven-wrapper). To compile it,
run `./mvnw compile`, or `./mvnw clean compile` if you want to clear any previous build information
before rebuilding the project.<br>
For windows users, use `mvnw.cmd` instead of `./mvnw`.

To run tests, use `./mvnw test`.

# Formatting

Not to worry too much on different coding styles, this code base is formatted
using [coveo fmt-maven-plugin](https://github.com/coveooss/fmt-maven-plugin) to apply Google style
guide.

To use it, run `./mvnw com.coveo:fmt-maven-plugin:format`.

# Build Jar for DataBricks

You have to build two jars if you want to launch jobs on databricks.

### Condition

Go to src/java/io/atoti/spark/condition/ and run this command :
```shell
javac -cp <path_to_jar>\* -source 1.8 -target 1.8 QueryCondition.java FalseCondition.java TrueCondition.java NotCondition.java NullCondition.java EqualCondition.java AndCondition.java OrCondition.java
```
With <path_to_jar> the path given by the command `databricks-connect get-jar-dir`.

Then, create a jar in out/artifacts/condition/ called condition.jar  
Into this jar, create this structure: io/atoti/spark/condition/ and put the all the condition class files inside

### Aggregation

Go to src/java/io/atoti/spark/aggregation/ and run this command :
```shell
javac -cp C:\Users\arnau\anaconda3\envs\databricks\Lib\site-packages\pyspark\jars\* -source 1.8 -target 1.8 AggregatedValue.java Avg.java Count.java Max.java Min.java Sum.java
```
With <path_to_jar> the path given by the command `databricks-connect get-jar-dir`.

Then, create a jar in out/artifacts/aggregation/ called condition.jar  
Into this jar, create this structure: io/atoti/spark/aggregation/ and put the all the aggregation class files inside