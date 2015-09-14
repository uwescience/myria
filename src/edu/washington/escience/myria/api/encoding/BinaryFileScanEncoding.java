package edu.washington.escience.myria.api.encoding;

import javax.annotation.Nonnull;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.io.DataSource;
import edu.washington.escience.myria.operator.BinaryFileScan;

public class BinaryFileScanEncoding extends LeafOperatorEncoding<BinaryFileScan> {
  @Required
  public Schema schema;
  @Required
  public DataSource source;
  public Boolean isLittleEndian;

  @Override
  public BinaryFileScan construct(@Nonnull final ConstructArgs args) {
    if (isLittleEndian == null) {
      return new BinaryFileScan(schema, source);
    } else {
      return new BinaryFileScan(schema, source, isLittleEndian);
    }
  }

}