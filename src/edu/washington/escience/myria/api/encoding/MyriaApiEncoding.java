package edu.washington.escience.myria.api.encoding;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.List;

import javax.ws.rs.core.Response.Status;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import edu.washington.escience.myria.api.MyriaApiException;

/**
 * The interface that all encodings must support. In particular, the validate function must throw an exception
 * explaining why the deserialized object is incorrect.
 *
 *
 */
public abstract class MyriaApiEncoding {
  /**
   * @return a list of required fields.
   */
  @JsonIgnore
  private final List<Field> getRequiredFields() {
    Field[] fs = this.getClass().getFields();
    List<Field> requiredFields = new LinkedList<>();
    for (Field f : fs) {
      Annotation a = f.getAnnotation(Required.class);
      if (a != null) {
        requiredFields.add(f);
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
    final List<String> missing = Lists.newLinkedList();
    /* Check using Java reflection to see that every field is there. */
    try {
      for (final Field f : getRequiredFields()) {
        if (null == f.get(this)) {
          missing.add(f.getName());
        }
      }
    } catch (IllegalAccessException e) {
      throw new MyriaApiException(Status.INTERNAL_SERVER_ERROR, e);
    }
    if (!missing.isEmpty()) {
      /* Some field is missing; throw a 400 (BAD REQUEST) exception with the list of required fields. */
      final StringBuilder sb =
          new StringBuilder(getClass().getName()).append(" is missing required fields: ");
      sb.append(Joiner.on(", ").join(missing)).append(".\n");
      throw new MyriaApiException(Status.BAD_REQUEST, sb.toString());
    }
    validateExtra();
  }

  /**
   * Adaptor function for extending operators to use if they wish to add extra validation beyond the list of required
   * fields.
   */
  protected void validateExtra() throws MyriaApiException {}
}
