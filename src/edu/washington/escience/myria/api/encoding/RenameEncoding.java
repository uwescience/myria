package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.Rename;
import edu.washington.escience.myria.parallel.Server;

public class RenameEncoding extends OperatorEncoding<Rename> {
  public String argChild;
  public List<String> columnNames;
  private static final List<String> requiredArguments = ImmutableList.of("argChild", "columnNames");

  @Override
  public void connect(final Operator current, final Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(argChild) });
  }

  @Override
  public Rename construct(final Server server) {
    return new Rename(columnNames);
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }

}
