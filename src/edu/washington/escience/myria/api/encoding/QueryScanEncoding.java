package edu.washington.escience.myria.api.encoding;

import java.util.Map;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.parallel.Server;

public class QueryScanEncoding extends OperatorEncoding<DbQueryScan> {
  @Required
  public Schema schema;
  @Required
  public String sql;

  @Override
  public DbQueryScan construct(final Server server) {
    return new DbQueryScan(sql, schema);
  }

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    /* Do nothing; no children. */
  }
}