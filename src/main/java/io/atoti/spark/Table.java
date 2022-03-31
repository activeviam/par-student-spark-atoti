package io.atoti.spark;

public class Table implements Queryable {
  String name;

  public Table(String name) {
    this.name = name;
  }

  @Override
  public String toSqlQuery() {
    return this.name;
  }
}
