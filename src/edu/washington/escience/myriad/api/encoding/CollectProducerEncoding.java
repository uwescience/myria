package edu.washington.escience.myriad.api.encoding;

import java.util.Map;

import javax.ws.rs.core.Response.Status;

import com.google.common.base.Preconditions;

import edu.washington.escience.myriad.api.MyriaApiException;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.parallel.CollectProducer;
import edu.washington.escience.myriad.parallel.ExchangePairID;

public class CollectProducerEncoding extends OperatorEncoding<CollectProducer> {
  public String argChild;
  public Integer argWorkerId;
  public Integer argOperatorId;

  @Override
  public void validate() throws MyriaApiException {
    super.validate();
    try {
      Preconditions.checkNotNull(argChild);
      Preconditions.checkNotNull(argWorkerId);
      Preconditions.checkNotNull(argOperatorId);
    } catch (Exception e) {
      throw new MyriaApiException(Status.BAD_REQUEST, "required fields: arg_child, arg_worker_id, arg_operator_id");
    }
  }

  @Override
  public CollectProducer construct() {
    return new CollectProducer(null, ExchangePairID.fromExisting(argOperatorId), argWorkerId);
  }

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(argChild) });
  }
}