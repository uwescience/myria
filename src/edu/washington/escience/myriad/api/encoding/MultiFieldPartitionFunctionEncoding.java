package edu.washington.escience.myriad.api.encoding;

import javax.ws.rs.core.Response.Status;

import com.google.common.base.Preconditions;

import edu.washington.escience.myriad.api.MyriaApiException;
import edu.washington.escience.myriad.parallel.MultiFieldHashPartitionFunction;

public class MultiFieldPartitionFunctionEncoding extends PartitionFunctionEncoding<MultiFieldHashPartitionFunction> {

  public int[] index;

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
  public MultiFieldHashPartitionFunction construct(int numPartitions) {
    MultiFieldHashPartitionFunction pf = new MultiFieldHashPartitionFunction(numPartitions);
    pf.setAttribute(MultiFieldHashPartitionFunction.FIELD_INDEX, index);
    return pf;
  }
}