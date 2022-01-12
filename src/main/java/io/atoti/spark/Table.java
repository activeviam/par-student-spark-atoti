package io.atoti.spark;

public record Table(String name) implements Queryable {

  @Override
  public String toSqlQuery() {
    return this.name;
  }
}
