package edu.washington.escience.myria.api.encoding;

import javax.annotation.Nonnull;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.DbQueryScan;

public class QueryScanEncoding extends LeafOperatorEncoding<DbQueryScan> {
  @Required
  public Schema schema;
  @Required
  public String sql;

  @Override
  public DbQueryScan construct(@Nonnull ConstructArgs args) {
    return new DbQueryScan(sql, schema);
  }

}