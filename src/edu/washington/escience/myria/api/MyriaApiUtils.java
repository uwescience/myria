package edu.washington.escience.myria.api;

import javax.ws.rs.core.CacheControl;
import javax.ws.rs.core.Response.ResponseBuilder;

/**
 * Helper functions for the API.
 */
public final class MyriaApiUtils {
  /** Utility class cannot be constructed. */
  private MyriaApiUtils() {}

  /**
   * Get a "do not cache" {@link CacheControl}.
   *
   * @return a {@link CacheControl} set to disable caching.
   */
  public static CacheControl doNotCache() {
    CacheControl noCache = new CacheControl();
    noCache.setNoCache(true);
    return noCache;
  }

  /**
   * Set the <code>Cache-Control</code> header in the specified cache to have the <code>no-cache</code> option set.
   *
   * @param response the response builder.
   * @return the response builder.
   */
  public static ResponseBuilder doNotCache(final ResponseBuilder response) {
    return response.cacheControl(doNotCache());
  }
}
