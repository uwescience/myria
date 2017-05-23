/**
 *
 */
package edu.washington.escience.myria.api.encoding;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.MyriaConstants.FunctionLanguage;
import edu.washington.escience.myria.accessmethod.SQLiteAccessMethod;
import edu.washington.escience.myria.Schema;

/**
 *
 */
public class FunctionStatus {
  /** The logger for this class. Uses SQLiteAccessMethod settings. */
  private static final Logger LOGGER = LoggerFactory.getLogger(SQLiteAccessMethod.class);
  /**
   * Instantiate a FunctionStatus with the provided values.
   *
   * @param name function name identifying the function.
   * @param description function description.
   * @param outputType The {@link Schema} of the input to the function.
   * @param isMultiValued does function return multiple tuples?
   * @param lang language of the function.
   * @param binary base64 encoded function binary.
   */
  @JsonCreator
  public FunctionStatus(
      @JsonProperty("name") final String name,
      @JsonProperty("shortName") final String shortName,
      @JsonProperty("description") final String description,
      @JsonProperty("outputType") final String outputType,
      @JsonProperty("isMultiValued") final Boolean isMultiValued,
      @JsonProperty("lang") final FunctionLanguage lang,
      @JsonProperty("binary") final String binary) {
    this.name = name;
    this.shortName = shortName;
    this.description = description;
    this.outputType = outputType;
    this.isMultiValued = isMultiValued;
    this.lang = lang;
    this.binary = binary;
  }

  /** The name identifying the function. */
  @JsonProperty private final String name;
  /** The name identifying the function. */
  @JsonProperty private final String shortName;
  /** The text of the function */
  @JsonProperty private final String description;
  /** The type of the output tuples to the function. */
  @JsonProperty private final String outputType;
  /** Does the function return multiple tuples. */
  @JsonProperty private final Boolean isMultiValued;
  /** The language of the function */
  @JsonProperty private final FunctionLanguage lang;
  /** base64 encoded string of code binary. */
  @JsonProperty private final String binary;

  /**
   * @return The name identifying the function.
   */
  public String getName() {
    return name;
  }
  /**
   * @return The short name identifying the function.
   */
  public String getShortName() {
    return shortName;
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
  public Boolean getIsMultiValued() {
    return isMultiValued;
  }
  /**
   * @return the language of function
   */
  public FunctionLanguage getLanguage() {
    return lang;
  }
  /**
   * @return The type of the output tuples as string.
   */
  public String getOutputType() {
    return outputType;
  }

  /**
   * @return get binary(base64 encoded) associated with the function.
   */
  public String getBinary() {
    return binary;
  }
}
