package edu.washington.escience.myriad.api;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/query")
public final class ReceiveQuery {
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  public String postNewQuery(final String input) {
    return input;
  }
}
