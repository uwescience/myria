package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.EmptyRelation;

public class EmptyRelationEncoding extends LeafOperatorEncoding<EmptyRelation> {
  @Required public Schema schema;

  @Override
  public EmptyRelation construct(ConstructArgs args) {
    return EmptyRelation.of(schema);
  }
}
