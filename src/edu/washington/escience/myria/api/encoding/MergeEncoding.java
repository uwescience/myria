package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.operator.Merge;
import edu.washington.escience.myria.parallel.Server;

public class MergeEncoding extends NaryOperatorEncoding<Merge> {

  @Override
  public Merge construct(final Server server) {
    return new Merge(null, null, null);
  }
}