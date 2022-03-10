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

Don't forget to create the .env based on the .env.example if you use a databricks cluster !

# Formatting

Not to worry too much on different coding styles, this code base is formatted
using [coveo fmt-maven-plugin](https://github.com/coveooss/fmt-maven-plugin) to apply Google style
guide.

To use it, run `./mvnw com.coveo:fmt-maven-plugin:format`.

# Build Jar for DataBricks

You have to build two jars if you want to launch jobs on databricks. To do so, run this command :
```shell
./mvnw -P jar-creation clean package clean
```
Then, you can build the project and run the tests/benchmarks (you have to be on the databricks profile)