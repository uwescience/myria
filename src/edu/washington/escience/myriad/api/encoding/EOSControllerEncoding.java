package edu.washington.escience.myriad.api.encoding;

import java.util.Map;

import javax.ws.rs.core.Response.Status;

import com.google.common.base.Preconditions;

import edu.washington.escience.myriad.api.MyriaApiException;
import edu.washington.escience.myriad.operator.Merge;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.parallel.EOSController;
import edu.washington.escience.myriad.parallel.ExchangePairID;

public class EOSControllerEncoding extends OperatorEncoding<EOSController> {
  public int[] argWorkerIds;
  public int[] argIdbOperatorIds;
  public String[] argChildren;

  @Override
  public void validate() throws MyriaApiException {
    super.validate();
    try {
      Preconditions.checkNotNull(argWorkerIds);
      Preconditions.checkNotNull(argIdbOperatorIds);
      Preconditions.checkNotNull(argChildren);
    } catch (Exception e) {
      throw new MyriaApiException(Status.BAD_REQUEST, "required fields: arg_worker_ids, arg_idb_ids, arg_children");
    }
  }

  @Override
  public EOSController construct() {
    ExchangePairID tmp[] = new ExchangePairID[argIdbOperatorIds.length];
    for (int i = 0; i < tmp.length; ++i) {
      tmp[i] = ExchangePairID.fromExisting(argIdbOperatorIds[i]);
    }
    return new EOSController(null, tmp, argWorkerIds);
  }

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    Operator[] tmp = new Operator[argChildren.length];
    for (int i = 0; i < tmp.length; ++i) {
      tmp[i] = operators.get(argChildren[i]);
    }
    current.setChildren(new Operator[] { new Merge(tmp) });
  }
}