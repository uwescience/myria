package edu.washington.escience.myria.api.encoding;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.List;

import javax.ws.rs.core.Response.Status;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

import edu.washington.escience.myria.api.MyriaApiException;

/**
 * The interface that all encodings must support. In particular, the validate function must throw an exception
 * explaining why the deserialized object is incorrect.
 * 
 * 
 */
public abstract class MyriaApiEncoding {
  /**
   * @return the list of names of required fields.
   */
  @JsonIgnore
  private final List<String> getRequiredFields() {
    Field[] fs = this.getClass().getFields();
    List<String> requiredFields = new LinkedList<>();
    for (Field f : fs) {
      Annotation a = f.getAnnotation(Required.class);
      if (a != null) {
        requiredFields.add(f.getName());
      }
    }
    return requiredFields;
  }

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
    String current = "";
    /* Check using Java reflection to see that every field is there. */
    try {
      for (final String f : fields) {
        current = f;
        Preconditions.checkNotNull(getClass().getField(f).get(this));
      }
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new MyriaApiException(Status.INTERNAL_SERVER_ERROR, e);
    } catch (final NullPointerException e1) {
      /* Some field is missing; throw a 400 (BAD REQUEST) exception with the list of required fields. */
      final StringBuilder sb = new StringBuilder(getClass().getName()).append(" has required fields: ");
      sb.append(Joiner.on(", ").join(fields));
      sb.append(". Missing ").append(current);
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
