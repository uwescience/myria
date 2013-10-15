package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.ColumnSelect;
import edu.washington.escience.myria.parallel.Server;

/**
 * A JSON-able wrapper for the expected wire message for a new dataset.
 * 
 * @author leelee
 * 
 */
public class ColumnSelectEncoding extends OperatorEncoding<ColumnSelect> {

  public int[] argFieldList;
  public String argChild;
  private static final List<String> requiredArguments = ImmutableList.of("argFieldList", "argChild");

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(argChild) });
  }

  @Override
  public ColumnSelect construct(Server server) {
    try {
      return new ColumnSelect(argFieldList, null);
    } catch (DbException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }
}
