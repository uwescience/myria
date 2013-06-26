package edu.washington.escience.myriad.api;

import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.PropertyNamingStrategy;
import org.slf4j.LoggerFactory;

/**
 * This class generates a custom JSON {@link ObjectMapper} that will properly convert Java CamelCase field names to JSON
 * lower_case_with_underscores field names.
 * 
 * @author dhalperi
 * 
 */
@Provider
@Produces(MediaType.APPLICATION_JSON)
public final class MyriaObjectMapper implements ContextResolver<ObjectMapper> {
  /** The logger for this class. */
  @SuppressWarnings("unused")
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(MyriaObjectMapper.class);

  @Override
  public ObjectMapper getContext(final Class<?> type) {
    ObjectMapper mapper = new ObjectMapper();
    mapper.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);
    return mapper;
  }
}
