package edu.washington.escience.myria.perfenforce;

import java.util.List;

class QueryMetaData {
  int id;
  double slaRuntime;
  int idealClusterSize;
  List<Integer> runtimes;

  public QueryMetaData(final int id, final double slaRuntime) {
    this.id = id;
    this.slaRuntime = slaRuntime;
  }

  public QueryMetaData(final int id, final double slaRuntime, final int idealClusterSize, final List<Integer> runtimes) {
    this.id = id;
    this.slaRuntime = slaRuntime;
    this.idealClusterSize = idealClusterSize;
    this.runtimes = runtimes;
  }

  @Override
  public String toString() {
    return "id : " + id + "sla : " + slaRuntime + "ideal : " + idealClusterSize;
  }
}