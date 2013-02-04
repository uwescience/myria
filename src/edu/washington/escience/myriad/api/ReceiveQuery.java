package edu.washington.escience.myriad.api;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/**
 * Class that handles queries.
 * 
 * @author dhalperi
 */
@Path("/query")
public final class ReceiveQuery {
  /**
   * For now, simply echoes back its input.
   * 
   * @param input the payload of the POST request itself.
   * @return the payload.
   */
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  public String postNewQuery(final String input) {
    return input;
  }
}
