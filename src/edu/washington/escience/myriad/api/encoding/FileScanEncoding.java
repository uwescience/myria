package edu.washington.escience.myriad.api.encoding;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response.Status;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.api.MyriaApiException;
import edu.washington.escience.myriad.operator.FileScan;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.parallel.Server;

public class FileScanEncoding extends OperatorEncoding<FileScan> {
  public Schema schema;
  public String fileName;
  public Boolean isCommaSeparated;
  private static final List<String> requiredArguments = ImmutableList.of("schema", "fileName");

  @Override
  public FileScan construct(final Server server) {
    try {
      if (isCommaSeparated == null) {
        return new FileScan(fileName, schema);
      } else {
        return new FileScan(fileName, schema, isCommaSeparated);
      }
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