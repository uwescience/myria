package edu.washington.escience.myria.perfenforce;

import java.util.ArrayList;

class QueryMetaData {
  int id;
  double slaRuntime;
  int idealClusterSize;
  ArrayList<Integer> runtimes;

  public QueryMetaData(final int id, final int slaRuntime) {
    this.id = id;
    this.slaRuntime = slaRuntime;
  }

  public QueryMetaData(final int id, final int slaRuntime, final int idealClusterSize, final ArrayList<Integer> runtimes) {
    this.id = id;
    this.slaRuntime = slaRuntime;
    this.idealClusterSize = idealClusterSize;
    this.runtimes = runtimes;
  }
}