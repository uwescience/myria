package edu.washington.escience.myriad.api;

import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.ext.Provider;

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
      ObjectMapper mapper = new ObjectMapper();

      /*
       * These are recommended by Swagger devs but I want to hold and see what we want. Leaving the code here for future
       * use.
       */
      // mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
      // mapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
      // mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
      // mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

      /* Use the Web's property naming strategy. */
      mapper.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

      commonMapper = mapper;
    }
    super.setMapper(commonMapper);
  }
}