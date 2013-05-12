package edu.washington.escience.myriad.api.encoding;

import java.util.Map;

import javax.ws.rs.core.Response.Status;

import com.google.common.base.Preconditions;

import edu.washington.escience.myriad.api.MyriaApiException;
import edu.washington.escience.myriad.operator.LocalJoin;
import edu.washington.escience.myriad.operator.Operator;

public class LocalJoinEncoding extends OperatorEncoding<LocalJoin> {
  public String argChild1;
  public int[] argColumns1;
  public int[] argSelect1;
  public String argChild2;
  public int[] argColumns2;
  public int[] argSelect2;

  @Override
  public void validate() throws MyriaApiException {
    super.validate();
    try {
      Preconditions.checkNotNull(argChild1);
      Preconditions.checkNotNull(argColumns1);
      Preconditions.checkNotNull(argSelect1);
      Preconditions.checkNotNull(argChild2);
      Preconditions.checkNotNull(argColumns2);
      Preconditions.checkNotNull(argSelect2);
    } catch (Exception e) {
      throw new MyriaApiException(Status.BAD_REQUEST,
          "required fields: arg_child1, arg_child2, arg_columns1, arg_columns2, arg_select1, arg_select2");
    }
  }

  @Override
  public LocalJoin construct() {
    return new LocalJoin(null, null, argColumns1, argColumns2, argSelect1, argSelect2);
  }

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(argChild1), operators.get(argChild2) });
  }
}