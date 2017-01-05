package edu.washington.escience.myria.api.encoding;

import java.util.List;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.DbQueryScan;

public class QueryScanEncoding extends LeafOperatorEncoding<DbQueryScan> {
  @Required public Schema schema;
  @Required public String sql;
  @Required public List<RelationKey> sourceRelationKeys;
  public boolean debroadcast;

  @Override
  public DbQueryScan construct(ConstructArgs args) {
    return new DbQueryScan(sql, schema);
  }
}
