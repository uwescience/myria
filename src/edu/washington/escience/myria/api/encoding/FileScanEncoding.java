package edu.washington.escience.myria.api.encoding;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response.Status;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.operator.FileScan;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.parallel.Server;

public class FileScanEncoding extends OperatorEncoding<FileScan> {
  public Schema schema;
  public String fileName;
  public String delimiter;
  private static final List<String> requiredArguments = ImmutableList.of("schema", "fileName");

  @Override
  public FileScan construct(final Server server) {
    try {
      return new FileScan(fileName, schema, delimiter);
    } catch (FileNotFoundException e) {
      throw new MyriaApiException(Status.BAD_REQUEST, e);
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