package edu.washington.escience.myriad.api.encoding;

import javax.ws.rs.core.Response.Status;

import com.google.common.base.Preconditions;

import edu.washington.escience.myriad.EqualsPredicate;
import edu.washington.escience.myriad.api.MyriaApiException;

public class EqualsPredicateEncoding extends PredicateEncoding<EqualsPredicate> {

  public Integer argCompareIndex;
  public String argCompareValue;

  @Override
  public void validate() throws MyriaApiException {
    super.validate();
    try {
      Preconditions.checkNotNull(argCompareIndex);
      Preconditions.checkNotNull(argCompareValue);
    } catch (Exception e) {
      throw new MyriaApiException(Status.BAD_REQUEST, "required field: index");
    }
  }

  @Override
  public EqualsPredicate construct() {
    return new EqualsPredicate(argCompareIndex, argCompareValue);
  }

}
