package edu.washington.escience.myriad.api.encoding;

import java.util.List;

import javax.ws.rs.core.Response.Status;

import com.google.common.base.Preconditions;

import edu.washington.escience.myriad.WithinSumRangePredicate;
import edu.washington.escience.myriad.api.MyriaApiException;

public class WithinSumRangePredicateEncoding extends PredicateEncoding<WithinSumRangePredicate> {

  public Integer argCompareIndex;
  public List<Integer> argOperandIndices;

  @Override
  public void validate() throws MyriaApiException {
    super.validate();
    try {
      Preconditions.checkNotNull(argCompareIndex);
      Preconditions.checkNotNull(argOperandIndices);
    } catch (Exception e) {
      throw new MyriaApiException(Status.BAD_REQUEST, "required field: index");
    }
  }

  @Override
  public WithinSumRangePredicate construct() {
    return new WithinSumRangePredicate(argCompareIndex, argOperandIndices);
  }

}
