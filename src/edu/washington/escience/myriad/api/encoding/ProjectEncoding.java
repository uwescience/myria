package edu.washington.escience.myriad.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.Project;

/**
 * A JSON-able wrapper for the expected wire message for a new dataset.
 * 
 * @author leelee
 * 
 */
public class ProjectEncoding extends OperatorEncoding<Project> {

  public int[] argFieldList;
  public String argChild;
  private static final List<String> requiredArguments = ImmutableList.of("argFieldList", "argChild");

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(argChild) });
  }

  @Override
  public Project construct() {
    try {
      return new Project(argFieldList, null);
    } catch (DbException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }
}
