package edu.washington.escience.myria.api.encoding;

import java.io.PipedOutputStream;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.api.DatasetFormat;
import edu.washington.escience.myria.operator.DataOutput;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.parallel.Server;

public class DataOutputEncoding extends OperatorEncoding<DataOutput> {

  public String argChild;
  public String format;
  /*
   * Allocate the pipes by which the {@link DataOutput} operator will talk to the {@link StreamingOutput} object that
   * will stream data to the client.
   */
  @JsonIgnore
  public PipedOutputStream writerOutput;
  private static final List<String> requiredArguments = ImmutableList.of("argChild", "format");

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(argChild) });
  }

  @Override
  public DataOutput construct(Server server) {
    return new DataOutput(null, DatasetFormat.validateFormat(format));
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }
}