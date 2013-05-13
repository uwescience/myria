package edu.washington.escience.myriad.api.encoding;

import java.util.Map;

import javax.ws.rs.core.Response.Status;

import com.google.common.base.Preconditions;

import edu.washington.escience.myriad.api.MyriaApiException;
import edu.washington.escience.myriad.operator.LocalCountingJoin;
import edu.washington.escience.myriad.operator.Operator;

public class LocalCountingJoinEncoding extends OperatorEncoding<LocalCountingJoin> {
  public String argChild1;
  public int[] argColumns1;
  public String argChild2;
  public int[] argColumns2;

  @Override
  public void validate() throws MyriaApiException {
    super.validate();
    try {
      Preconditions.checkNotNull(argChild1);
      Preconditions.checkNotNull(argColumns1);
      Preconditions.checkNotNull(argChild2);
      Preconditions.checkNotNull(argColumns2);
    } catch (Exception e) {
      throw new MyriaApiException(Status.BAD_REQUEST,
          "required fields: arg_child1, arg_child2, arg_columns1, arg_columns2");
    }
  }

  @Override
  public LocalCountingJoin construct() {
    return new LocalCountingJoin(null, null, argColumns1, argColumns2);
  }

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(argChild1), operators.get(argChild2) });
  }
}