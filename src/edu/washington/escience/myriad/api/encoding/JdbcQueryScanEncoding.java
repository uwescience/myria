package edu.washington.escience.myriad.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.accessmethod.JdbcInfo;
import edu.washington.escience.myriad.operator.JdbcQueryScan;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.parallel.Server;

/**
 * 
 * Encoding of JdbcQueryScan
 * 
 * @author Shumo Chu <chushumo@cs.washington.edu>
 * 
 */
public class JdbcQueryScanEncoding extends OperatorEncoding<JdbcQueryScan> {

  /** schema of queried table */
  public Schema schema;

  /** query SQL */
  public String sql;

  /** JDBC info of the database where the relation is from */
  public JdbcInfo jdbcInfo;

  private static final List<String> requiredArguments = ImmutableList.of("schema", "sql", "jdbcInfo");

  @Override
  public JdbcQueryScan construct(final Server server) {
    return new JdbcQueryScan(jdbcInfo, sql, schema);
  }

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    /* Do nothing; no children. */
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }

}
