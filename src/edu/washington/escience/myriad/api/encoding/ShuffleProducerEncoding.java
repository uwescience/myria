package edu.washington.escience.myriad.api.encoding;

import java.util.Map;

import javax.ws.rs.core.Response.Status;

import com.google.common.base.Preconditions;

import edu.washington.escience.myriad.api.MyriaApiException;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.parallel.ExchangePairID;
import edu.washington.escience.myriad.parallel.ShuffleProducer;

/**
 * A JSON-able wrapper for the expected wire message for a new dataset.
 * 
 */
public class ShuffleProducerEncoding extends OperatorEncoding<ShuffleProducer> {
  public String argChild;
  public int[] argWorkerIds;
  public Integer argOperatorId;
  public PartitionFunctionEncoding<?> argPf;

  @Override
  public void validate() throws MyriaApiException {
    super.validate();
    try {
      Preconditions.checkNotNull(argChild);
      Preconditions.checkNotNull(argWorkerIds);
      Preconditions.checkNotNull(argOperatorId);
      Preconditions.checkNotNull(argPf);
    } catch (Exception e) {
      throw new MyriaApiException(Status.BAD_REQUEST,
          "required fields: arg_child, arg_worker_ids, arg_operator_id, arg_pf");
    }
  }

  @Override
  public ShuffleProducer construct() {
    return new ShuffleProducer(null, ExchangePairID.fromExisting(argOperatorId), argWorkerIds, argPf
        .construct(argWorkerIds.length));
  }

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(argChild) });
  }
}