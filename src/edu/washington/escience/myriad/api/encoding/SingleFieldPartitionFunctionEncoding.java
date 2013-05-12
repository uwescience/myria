package edu.washington.escience.myriad.api.encoding;

import javax.ws.rs.core.Response.Status;

import com.google.common.base.Preconditions;

import edu.washington.escience.myriad.api.MyriaApiException;
import edu.washington.escience.myriad.parallel.SingleFieldHashPartitionFunction;

public class SingleFieldPartitionFunctionEncoding extends PartitionFunctionEncoding<SingleFieldHashPartitionFunction> {
  public Integer index;

  @Override
  public void validate() throws MyriaApiException {
    super.validate();
    try {
      Preconditions.checkNotNull(index);
    } catch (Exception e) {
      throw new MyriaApiException(Status.BAD_REQUEST, "required field: index");
    }
  }

  @Override
  public SingleFieldHashPartitionFunction construct(int numPartitions) {
    SingleFieldHashPartitionFunction pf = new SingleFieldHashPartitionFunction(numPartitions);
    pf.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, index);
    return pf;
  }
}