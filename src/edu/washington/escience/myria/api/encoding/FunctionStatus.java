/**
 *
 */
package edu.washington.escience.myria.api.encoding;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.MyriaConstants.FunctionLanguage;
import edu.washington.escience.myria.Schema;

/**
 *
 */
public class FunctionStatus {
  /**
   * Instantiate a FunctionStatus with the provided values.
   *
   * @param name function name identifying the function.
   * @param description function description.
   * @param outputType The {@link Schema} of the input to the function.
   * @param isMultiValued does function return multiple tuples?
   * @param lang language of the function.
   */
  @JsonCreator
  public FunctionStatus(
      @JsonProperty("name") final String name,
      @JsonProperty("description") final String description,
      @JsonProperty("outputType") final String outputType,
      @JsonProperty("isMultivalued") final Boolean isMultivalued,
      @JsonProperty("lang") final MyriaConstants.FunctionLanguage lang) {
    this.name = name;
    this.description = description;
    this.outputType = outputType;
    this.isMultivalued = isMultivalued;
    this.lang = lang;
  }

  /** The name identifying the function. */
  @JsonProperty private final String name;
  /** The text of the function */
  @JsonProperty private final String description;

  /** The {@link Schema} of the output tuples to the function. */
  @JsonProperty private final String outputType;

  /** Does the function return multiple tuples. */
  @JsonProperty private final Boolean isMultivalued;

  /** The language the function */
  @JsonProperty private final MyriaConstants.FunctionLanguage lang;
  /**
   * @return The name identifying the function.
   */
  public String getName() {
    return name;
  }
  /**
   * @return get text associated with the function
   */
  public String getDescription() {
    return description;
  }

  /**
   * @return get if function is multivalued.
   */
  public Boolean getIsMultivalued() {
    return isMultivalued;
  }
  /**
   * @return The {@link Schema} of the output tuples in the function.
   */
  public String getOutputType() {
    return outputType;
  }

  /**
   * @return the language of function
   */
  public FunctionLanguage getLanguage() {
    return lang;
  }
}
