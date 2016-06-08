package edu.washington.escience.myria.api;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import edu.washington.escience.myria.api.encoding.VersionEncoding;

/**
 * This is the class that handles API calls that return workers.
 *
 * @author jwang
 */
@Path("/server")
public final class MasterResource {
  /**
   * Get the version information of Myria at build time.
   *
   * @return a {@link VersionEncoding}.
   */
  @GET
  @Produces(MyriaApiConstants.JSON_UTF_8)
  public Response getVersion() {
    return Response.ok(new VersionEncoding()).build();
  }
}
