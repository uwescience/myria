package edu.washington.escience.myria.api.encoding;

import java.util.Map;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.operator.BinaryFileScan;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.parallel.Server;

public class BinaryFileScanEncoding extends OperatorEncoding<BinaryFileScan> {
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

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    /* Do nothing; no children. */
  }

}