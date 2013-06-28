package edu.washington.escience.myriad.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.operator.BinaryFileScan;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.parallel.Server;

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