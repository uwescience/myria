package edu.washington.escience.myria.api.encoding;

import java.util.Map;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.io.DataSource;
import edu.washington.escience.myria.operator.FileScan;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.parallel.Server;

public class FileScanEncoding extends OperatorEncoding<FileScan> {
  @Required
  public Schema schema;
  @Required
  public DataSource source;
  public Character delimiter;
  public Character quote;
  public Character escape;
  public Integer skip;

  @Override
  public FileScan construct(final Server server) {
    return new FileScan(source, schema, delimiter, quote, escape, skip);
  }

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    /* Do nothing; no children. */
  }
}