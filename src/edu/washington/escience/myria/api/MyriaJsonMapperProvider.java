package edu.washington.escience.myria.api;

import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.ext.Provider;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;

/**
 * This class generates a custom JSON {@link ObjectMapper} that will properly convert Java CamelCase field names to JSON
 * lower_case_with_underscores field names.
 * 
 * @author dhalperi
 * 
 */
@Provider
@Produces(MediaType.APPLICATION_JSON)
public class MyriaJsonMapperProvider extends JacksonJaxbJsonProvider {
  /** Only create this object once, and share it among instances. */
  private static ObjectMapper commonMapper = null;

  /** Get (or create) the custom ObjectMapper and then set it in the parent. */
  public MyriaJsonMapperProvider() {
    if (commonMapper == null) {
      commonMapper = newMapper();
    }
    super.setMapper(commonMapper);
  }

  /**
   * Create the standard Myria custom ObjectMapper.
   * 
   * @return the standard Myria custom ObjectMapper.
   */
  public static ObjectMapper newMapper() {
    ObjectMapper mapper = new ObjectMapper();

    /*
     * These are recommended by Swagger devs but I want to hold and see what we want. Leaving the code here for future
     * use.
     */
    // mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    // mapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
    // mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    // mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    /* Don't automatically detect getters, explicit is better than implicit. */
    mapper.setVisibility(PropertyAccessor.GETTER, Visibility.NONE);
    mapper.setVisibility(PropertyAccessor.IS_GETTER, Visibility.NONE);
    mapper.setVisibility(PropertyAccessor.SETTER, Visibility.NONE);

    /* Use the Web's property naming strategy. */
    mapper.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

    return mapper;
  }
}