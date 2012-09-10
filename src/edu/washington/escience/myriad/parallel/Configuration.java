package edu.washington.escience.myriad.parallel;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

// TODO: stub
public class Configuration {

  public static final String CONF_DIR = "conf";
  private final SocketInfo server;
  private final SocketInfo[] workers;

  // private final String confDir;

  public Configuration(String confDir) throws IOException {
    // this.confDir = confDir;
    this.server = loadServer(confDir);
    this.workers = loadWorkers(confDir);
  }

  public Configuration() throws IOException {
    this(CONF_DIR);
  }

  public SocketInfo getServer() {
    return this.server;
  }

  public SocketInfo[] getWorkers() {
    return this.workers;
  }

  protected static SocketInfo[] loadWorkers(String confDir) throws IOException {
    ArrayList<SocketInfo> workers = new ArrayList<SocketInfo>();
    BufferedReader br =
        new BufferedReader(new InputStreamReader(new FileInputStream(new File(confDir + "/workers.conf"))));
    String line = null;
    while ((line = br.readLine()) != null) {
      String[] ts = line.replaceAll("[ \t]+", "").replaceAll("#.*$", "").split(":");
      if (ts.length >= 2)
        workers.add(new SocketInfo(ts[0], Integer.parseInt(ts[1])));
    }
    return workers.toArray(new SocketInfo[] {});
  }

  protected static SocketInfo loadServer(String confDir) throws IOException {
    BufferedReader br =
        new BufferedReader(new InputStreamReader(new FileInputStream(new File(confDir + "/server.conf"))));
    String line = null;
    while ((line = br.readLine()) != null) {
      String[] ts = line.replaceAll("[ \t]+", "").replaceAll("#.*$", "").split(":");
      if (ts.length == 2)
        return new SocketInfo(ts[0], Integer.parseInt(ts[1]));
    }
    throw new IOException("Wrong server conf file.");
  }

}
