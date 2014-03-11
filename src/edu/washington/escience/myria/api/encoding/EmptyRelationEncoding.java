package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.operator.EmptyRelation;
import edu.washington.escience.myria.parallel.Server;

public class EmptyRelationEncoding extends LeafOperatorEncoding<EmptyRelation> {
  @Required
  public Schema schema;

  @Override
  public EmptyRelation construct(final Server server) {
    return EmptyRelation.of(schema);
  }
}