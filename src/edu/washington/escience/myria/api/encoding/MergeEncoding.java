package edu.washington.escience.myria.api.encoding;

import javax.annotation.Nonnull;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.Merge;

public class MergeEncoding extends NaryOperatorEncoding<Merge> {

  @Override
  public Merge construct(@Nonnull ConstructArgs args) {
    return new Merge(null, null, null);
  }
}