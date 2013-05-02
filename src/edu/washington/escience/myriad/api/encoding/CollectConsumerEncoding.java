package edu.washington.escience.myriad.api.encoding;

import java.util.Map;

import javax.ws.rs.core.Response.Status;

import com.google.common.base.Preconditions;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.api.MyriaApiException;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.parallel.CollectConsumer;
import edu.washington.escience.myriad.parallel.ExchangePairID;

public class CollectConsumerEncoding extends OperatorEncoding<CollectConsumer> {
  public Schema argSchema;
  public int[] argWorkerIds;
  public Integer argOperatorId;

  @Override
  public void validate() throws MyriaApiException {
    super.validate();
    try {
      Preconditions.checkNotNull(argSchema);
      Preconditions.checkNotNull(argWorkerIds);
      Preconditions.checkNotNull(argOperatorId);
    } catch (Exception e) {
      throw new MyriaApiException(Status.BAD_REQUEST, "required fields: arg_schema, arg_worker_ids, arg_operator_id");
    }
  }

  @Override
  public CollectConsumer construct() {
    return new CollectConsumer(argSchema, ExchangePairID.fromExisting(argOperatorId), argWorkerIds);
  }

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    /* Do nothing. */
  }
}