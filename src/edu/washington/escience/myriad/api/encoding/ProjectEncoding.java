package edu.washington.escience.myriad.api.encoding;

import java.util.Map;

import javax.ws.rs.core.Response.Status;

import com.google.common.base.Preconditions;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.api.MyriaApiException;
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

  @Override
  public void validate() {
    super.validate();
    try {
      Preconditions.checkNotNull(argFieldList);
      Preconditions.checkNotNull(argChild);
    } catch (Exception e) {
      throw new MyriaApiException(Status.BAD_REQUEST, "required fields: arg_field_list, arg_child");
    }
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
  public void connect(Operator current, Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(argChild) });
  }
}
