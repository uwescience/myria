package edu.washington.escience.myriad.api;

import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.core.Application;

public class MasterApplication extends Application {
  @Override
  public Set<Class<?>> getClasses() {
    final Set<Class<?>> rrcs = new HashSet<Class<?>>();
    rrcs.add(WorkerCollection.class);
    return rrcs;
  }
}