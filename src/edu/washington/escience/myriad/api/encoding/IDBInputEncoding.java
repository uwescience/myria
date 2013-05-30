package edu.washington.escience.myriad.api.encoding;

import java.util.Map;

import javax.ws.rs.core.Response.Status;

import com.google.common.base.Preconditions;

import edu.washington.escience.myriad.api.MyriaApiException;
import edu.washington.escience.myriad.operator.IDBInput;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.parallel.ExchangePairID;

public class IDBInputEncoding extends OperatorEncoding<IDBInput> {
  public int argControllerOperatorId;
  public int argControllerWorkerId;
  public int argSelfIdbId;
  public String argInitialInput;
  public String argIterationInput;
  public String argEosControllerInput;

  @Override
  public void validate() throws MyriaApiException {
    super.validate();
    try {
      Preconditions.checkNotNull(argSelfIdbId);
      Preconditions.checkNotNull(argControllerOperatorId);
      Preconditions.checkNotNull(argControllerWorkerId);
      Preconditions.checkNotNull(argInitialInput);
      Preconditions.checkNotNull(argIterationInput);
      Preconditions.checkNotNull(argEosControllerInput);
    } catch (Exception e) {
      throw new MyriaApiException(Status.BAD_REQUEST, "required fields: arg_worker_ids, arg_idb_ids, arg_child");
    }
  }

  @Override
  public IDBInput construct() {
    return new IDBInput(argSelfIdbId, ExchangePairID.fromExisting(argControllerOperatorId), argControllerWorkerId,
        null, null, null);
  }

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    current.setChildren(new Operator[] {
        operators.get(argInitialInput), operators.get(argIterationInput), operators.get(argEosControllerInput) });
  }
}