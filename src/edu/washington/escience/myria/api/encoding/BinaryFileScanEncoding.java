package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.operator.BinaryFileScan;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.parallel.Server;

public class BinaryFileScanEncoding extends OperatorEncoding<BinaryFileScan> {
  public Schema schema;
  public String fileName;
  public Boolean isLittleEndian;
  private static final List<String> requiredArguments = ImmutableList.of("schema", "fileName");

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

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }
}