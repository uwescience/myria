package edu.washington.escience.myriad.api.encoding;

import edu.washington.escience.myriad.api.MyriaApiException;

/**
 * The interface that all encodings must support. In particular, the validate function must throw an exception
 * explaining why the deserialized object is incorrect.
 * 
 * @author dhalperi
 * 
 */
public interface MyriaApiEncoding {
  /**
   * Checks that this deserialized instance passes input validation. One common invariant is that no fields be missing.
   * 
   * @throws MyriaApiException if the validation checks fail.
   */
  public void validate() throws MyriaApiException;
}
