package edu.washington.escience.myria.api.encoding;

import java.util.List;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.DbQueryScan;

public abstract class AbstractQueryScanEncoding extends LeafOperatorEncoding<DbQueryScan> {
  /** If it needs to be debroadcasted. */
  public boolean debroadcast;

  /**
   * @param args
   * @return the list of relation keys being touched.
   */
  public abstract List<RelationKey> sourceRelationKeys(ConstructArgs args);
}
