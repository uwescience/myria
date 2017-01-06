package edu.washington.escience.myria.api.encoding;

import java.util.List;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.DbQueryScan;

public class QueryScanEncoding extends AbstractQueryScanEncoding {
  @Required public Schema schema;
  @Required public String sql;
  @Required public List<RelationKey> sourceRelationKeys;

  public List<RelationKey> sourceRelationKeys(ConstructArgs args) {
    return sourceRelationKeys;
  }

  @Override
  public DbQueryScan construct(ConstructArgs args) {
    return new DbQueryScan(sql, schema);
  }
}
