/**
 *
 */
package edu.washington.escience.myria.api.encoding;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.Schema;

/**
 *
 */
public class FunctionStatus {
  /**
   * Instantiate a FunctionStatus with the provided values.
   *
   * @param name function name identifying the function.
   * @param inputSchema The {@link Schema} of the input to the function.
   * @param outputSchema The {@link Schema} of the input to the function.
   * @param text Text associated with the function.
   * @param lang language of the function.
   */
  @JsonCreator
  public FunctionStatus(
      @JsonProperty("name") final String name,
      @JsonProperty("outputType") final String outputType,
      @JsonProperty("description") final String text,
      @JsonProperty("lang") final MyriaConstants.FunctionLanguage lang) {
    this.name = name;
    this.outputType = outputType;
    this.definition = text;
    this.lang = lang;
  }

  /** The name identifying the function. */
  @JsonProperty private final String name;
  /** The {@link Schema} of the output tuples to the function. */
  @JsonProperty private final String outputType;
  /** The text of the function */
  @JsonProperty private final String definition;
  /** The text of the function */
  @JsonProperty private final MyriaConstants.FunctionLanguage lang;
  /**
   * @return The name identifying the function.
   */
  public String getName() {
    return name;
  }

  /**
   * @return The {@link Schema} of the output tuples in the function.
   */
  public String getOutputType() {
    return outputType;
  }

  /**
   * @return get text associated with the function
   */
  public String getDefinition() {
    return definition;
  }

  /**
   * @return the language of function
   */
  public MyriaConstants.FunctionLanguage getLanguage() {
    return lang;
  }
}
