package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.operator.BinaryFileScan;
import edu.washington.escience.myria.parallel.Server;

public class BinaryFileScanEncoding extends LeafOperatorEncoding<BinaryFileScan> {
  @Required
  public Schema schema;
  @Required
  public String fileName;
  public Boolean isLittleEndian;

  @Override
  public BinaryFileScan construct(final Server server) {
    if (isLittleEndian == null) {
      return new BinaryFileScan(schema, fileName);
    } else {
      return new BinaryFileScan(schema, fileName, isLittleEndian);
    }
  }

}