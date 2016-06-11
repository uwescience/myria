package edu.washington.escience.myria.perfenforce;

import java.util.List;

class QueryMetaData {
  int id;
  double slaRuntime;
  int idealClusterSize;
  List<Double> runtimes;

  public QueryMetaData(final int id, final double slaRuntime) {
    this.id = id;
    this.slaRuntime = slaRuntime;
  }

  public QueryMetaData(final int id, final double slaRuntime, final int idealClusterSize, final List<Double> runtimes) {
    this.id = id;
    this.slaRuntime = slaRuntime;
    this.idealClusterSize = idealClusterSize;
    this.runtimes = runtimes;
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