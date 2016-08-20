package edu.washington.escience.myria.perfenforce;

import edu.washington.escience.myria.api.encoding.Required;

public class QueryMetaData {
  @Required public int id;
  @Required public double slaRuntime;
  @Required public String description;

  public QueryMetaData() {}

  public QueryMetaData(final int id, final double slaRuntime) {
    this.id = id;
    this.slaRuntime = slaRuntime;
  }

  public double getSLA() {
    return slaRuntime;
  }
}
