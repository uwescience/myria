package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.parallel.Server;

public class RawQueryScanEncoding extends LeafOperatorEncoding<DbQueryScan> {
  @Required
  public Schema schema;
  @Required
  public String sql;

  @Override
  public DbQueryScan construct(final Server server) {
    return new DbQueryScan(sql, schema);
  }
}