package edu.washington.escience.myria.api;

import java.io.IOException;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;

import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.InstanceStateChange;
import com.amazonaws.services.ec2.model.StartInstancesRequest;
import com.amazonaws.services.ec2.model.StartInstancesResult;
import com.wordnik.swagger.annotations.Api;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.coordinator.ConfigFileException;
import edu.washington.escience.myria.daemon.MyriaDriver;

/*
 * TEST TEST TEST
 */
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MyriaApiConstants.JSON_UTF_8)
@Path("/driver")
@Api(value = "/driver", description = "Adding calls to the driver")
public class DriverAddWorker {

  /** The Myria server running on the master. */
  @Context private MyriaDriver driver;

  /** Logger. */
  protected static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(DriverAddWorker.class);

  /**
   * @throws InjectionException
   * @throws IOException
   * @throws BindException
   * @throws InterruptedException
   */
  @GET
  @Path("/test/")
  public Response test()
      throws DbException, BindException, IOException, InjectionException, InterruptedException,
          ConfigFileException {
    LOGGER.warn("HERE");
    /* Build the response to return the queryId */

    //Call some worker from Amazon here and start it
    while (true) {
     StartInstancesRequest request = new StartInstancesRequest().withInstanceIds("ID HERE");
	 AmazonEC2Client client = new AmazonEC2Client(new DefaultAWSCredentialsProviderChain());
	 client.setRegion(Region.getRegion(Regions.US_WEST_2));
	 StartInstancesResult startInstancesResult = client.startInstances(request);
	 List<InstanceStateChange> listStates = startInstancesResult.getStartingInstances();
      boolean isDone = false;
      for (InstanceStateChange l : listStates) {
        LOGGER.warn("STATUS " + l.getCurrentState().getName());
        if (l.getCurrentState().getName().equals("running")) {
          isDone = true;
        }
      }
      if (isDone) {
        break;
      }
    }

    ResponseBuilder response = Response.ok();
    int workerCount = driver.fakeAdditionalWorker();
    return response.entity(workerCount).build();
  }
}
