package edu.washington.escience.myriad.api;

import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.core.Application;

/**
 * This object simply contains the list of resources that can be requested via the RESTlet server.
 * 
 * @author dhalperi
 */
public class MasterApplication extends Application {
  @Override
  public final Set<Class<?>> getClasses() {
    final Set<Class<?>> rrcs = new HashSet<Class<?>>();
    rrcs.add(QueryResource.class);
    rrcs.add(DatasetResource.class);
    rrcs.add(WorkerCollection.class);
    return rrcs;
  }
}