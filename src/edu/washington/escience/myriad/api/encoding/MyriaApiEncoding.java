package edu.washington.escience.myriad.api.encoding;

import java.util.List;

import javax.ws.rs.core.Response.Status;

import org.codehaus.jackson.map.PropertyNamingStrategy.LowerCaseWithUnderscoresStrategy;

import com.google.common.base.Preconditions;

import edu.washington.escience.myriad.api.MyriaApiException;

/**
 * The interface that all encodings must support. In particular, the validate function must throw an exception
 * explaining why the deserialized object is incorrect.
 * 
 * @author dhalperi
 * 
 */
public abstract class MyriaApiEncoding {
  /**
   * @return the list of names of required fields.
   */
  protected abstract List<String> getRequiredFields();

  /**
   * Checks that this deserialized instance passes input validation. First, it enforces that all required fields
   * (defined by the return value from getRequiredFields()) are present. Second, it calls the child's method
   * validateExtra().
   * 
   * @throws MyriaApiException if the validation checks fail.
   */
  public final void validate() throws MyriaApiException {
    /* Fetch the list of required fields. */
    final List<String> fields = getRequiredFields();
    /* Check using Java reflection to see that every field is there. */
    try {
      for (final String f : fields) {
        Preconditions.checkNotNull(getClass().getField(f).get(this));
      }
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new MyriaApiException(Status.INTERNAL_SERVER_ERROR, e);
    } catch (final NullPointerException e1) {
      /* Some field is missing; throw a 400 (BAD REQUEST) exception with the list of required fields. */
      final LowerCaseWithUnderscoresStrategy translator = new LowerCaseWithUnderscoresStrategy();
      final StringBuilder sb = new StringBuilder(getClass().getName()).append(" has required fields: ");
      boolean first = true;
      for (final String f : fields) {
        if (!first) {
          sb.append(", ");
        }
        first = false;
        sb.append(translator.translate(f));
      }
      throw new MyriaApiException(Status.BAD_REQUEST, sb.toString());
    }
    validateExtra();
  }

  /**
   * Adaptor function for extending operators to use if they wish to add extra validation beyond the list of required
   * fields.
   */
  protected void validateExtra() throws MyriaApiException {
  }
}
