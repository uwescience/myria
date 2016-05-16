package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.io.DataSource;
import edu.washington.escience.myria.operator.FileScan;

public class FileScanEncoding extends LeafOperatorEncoding<FileScan> {
  @Required public Schema schema;
  @Required public DataSource source;
  public Character delimiter;
  public Character quote;
  public Character escape;
  public Integer skip;

  @Override
  public FileScan construct(ConstructArgs args) {
    return new FileScan(source, schema, delimiter, quote, escape, skip);
  }
}
