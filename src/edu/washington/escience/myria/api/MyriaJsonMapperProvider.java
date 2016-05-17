package edu.washington.escience.myria.api;

import javax.ws.rs.Produces;
import javax.ws.rs.ext.Provider;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;

/**
 * This class generates a custom JSON {@link ObjectMapper} that will properly convert Java CamelCase field names to JSON
 * lower_case_with_underscores field names.
 *
 *
 */
@Provider
@Produces(MyriaApiConstants.JSON_UTF_8)
public class MyriaJsonMapperProvider extends JacksonJaxbJsonProvider {
  /** Only create this object once, and share it among instances. */
  private static final ObjectMapper MAPPER = newMapper();

  /** Get (or create) the custom ObjectMapper and then set it in the parent. */
  public MyriaJsonMapperProvider() {
    super.setMapper(MAPPER);
  }

  /**
   * @return An {@link ObjectMapper} that fits Myria's customizations.
   */
  public static ObjectMapper getMapper() {
    return MAPPER;
  }

  /**
   * @return An {@link ObjectReader} that fits Myria's customizations.
   */
  public static ObjectReader getReader() {
    return MAPPER.reader();
  }

  /**
   * @return An {@link ObjectWriter} that fits Myria's customizations.
   */
  public static ObjectWriter getWriter() {
    return MAPPER.writer();
  }

  /**
   * Create the standard Myria custom ObjectMapper.
   *
   * @return the standard Myria custom ObjectMapper.
   */
  private static ObjectMapper newMapper() {
    ObjectMapper mapper = new ObjectMapper();

    /* Serialize DateTimes as Strings */
    mapper.registerModule(new JodaModule());
    /* Serialize Guava types correctly */
    mapper.registerModule(new GuavaModule());

    /*
     * These are recommended by Swagger devs but I want to hold and see what we want. Leaving the code here for future
     * use.
     */
    // mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    // mapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
    mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    // mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    /* Don't automatically detect getters, explicit is better than implicit. */
    mapper.setVisibility(PropertyAccessor.GETTER, Visibility.NONE);
    mapper.setVisibility(PropertyAccessor.IS_GETTER, Visibility.NONE);
    mapper.setVisibility(PropertyAccessor.SETTER, Visibility.NONE);

    return mapper;
  }
}
