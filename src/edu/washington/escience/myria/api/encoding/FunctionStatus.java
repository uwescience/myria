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
  public FunctionStatus(@JsonProperty("name") final String name, @JsonProperty("inputSchema") final String inputSchema,
      @JsonProperty("outputType") final String outputType, @JsonProperty("text") final String text,
      @JsonProperty("lang") final MyriaConstants.FunctionLanguage lang) {
    this.name = name;
    this.outputType = outputType;
    this.inputSchema = inputSchema;
    this.text = text;
    this.lang = lang;

  }

  /** The name identifying the function. */
  @JsonProperty
  private final String name;
  /** The {@link Schema} of the output tuples to the function. */
  @JsonProperty
  private final String outputType;
  /** The {@link Schema} of the input tuples to the function. */
  @JsonProperty
  private final String inputSchema;
  /** The text of the function */
  @JsonProperty
  private final String text;
  /** The text of the function */
  @JsonProperty
  private final MyriaConstants.FunctionLanguage lang;

  // @JsonProperty public URI uri;

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
   * @return The {@link Schema} of the input tuples in the function.
   */
  public String getInputSchema() {
    return inputSchema;
  }

  /**
   * @return get text associated with the function
   */
  public String getText() {
    return text;
  }

  /**
   * @return the language of function
   */
  public MyriaConstants.FunctionLanguage getLanguage() {
    return lang;
  }

}
