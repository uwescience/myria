package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.DbQueryScan;

public class QueryScanEncoding extends AbstractQueryScanEncoding {
  @Required public Schema schema;
  @Required public String sql;
  @Required public List<RelationKey> sourceRelationKeys;

  public Set<RelationKey> sourceRelationKeys(ConstructArgs args) {
    return ImmutableSet.copyOf(sourceRelationKeys);
  }

  @Override
  public DbQueryScan construct(ConstructArgs args) {
    return new DbQueryScan(sql, schema);
  }
}
