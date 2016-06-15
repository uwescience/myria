package edu.washington.escience.myria.perfenforce;

import java.util.List;

import edu.washington.escience.myria.api.encoding.Required;

public class QueryMetaData {
  @Required
  public int id;
  @Required
  public double slaRuntime;
  @Required
  public String description;
  public int idealClusterSize;
  public List<Double> runtimes;

  public QueryMetaData() {
  }

  public QueryMetaData(final int id, final double slaRuntime) {
    this.id = id;
    this.slaRuntime = slaRuntime;
  }

  public QueryMetaData(final int id, final String description, final double slaRuntime, final int idealClusterSize,
      final List<Double> runtimes) {
    this.id = id;
    this.slaRuntime = slaRuntime;
    this.idealClusterSize = idealClusterSize;
    this.runtimes = runtimes;
    this.description = description;
  }

  public int getIdealClusterSize() {
    return idealClusterSize;
  }

  public double getSLA() {
    return slaRuntime;
  }

  @Override
  public String toString() {
    return "id : " + id + "sla : " + slaRuntime + "ideal : " + idealClusterSize;
  }
}