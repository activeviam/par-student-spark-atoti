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
