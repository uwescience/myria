package edu.washington.escience.myriad.systemtest.sinnglejvm;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

import javassist.CtClass;
import javassist.CtMethod;
import javassist.CtNewMethod;
import javassist.NotFoundException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import simpledb.BufferPool;
import simpledb.Database;
import simpledb.HeapFileEncoder;
import simpledb.HeapFileSplitter;
import simpledb.TableStats;
import simpledb.Type;
import simpledb.parallel.ParallelUtility;
import simpledb.parallel.SocketInfo;

public class ParallelTestBase {

    protected void beforeMethodCall(CtClass clas, String mname, String code)
            throws Exception {
        clas.defrost();
        CtMethod mold = clas.getDeclaredMethod(mname);
        int idx = 0;
        String nname = null;
        while (true) {
            nname = mname + "$impl" + (idx++);
            try {
                clas.getDeclaredMethod(nname);
            } catch (NotFoundException ee) {
                break;
            }
        }
        mold.setName(nname);
        CtMethod mnew = CtNewMethod.copy(mold, mname, clas, null);
        String type = mold.getReturnType().getName();

        StringBuffer body = new StringBuffer();
        body.append("{\n");
        body.append(code);
        if (!"void".equals(type)) {
            body.append(" return ");
        }
        body.append(nname + "($$);\n");
        body.append("}");

        mnew.setBody(body.toString());
        clas.addMethod(mnew);
    }

    //the return value of the original method is stored in a variable called result
    //only non-primitive return values are allowed
    protected void afterMethodCall(CtClass clas, String mname, String code)
            throws Exception {
        clas.defrost();
        CtMethod mold = clas.getDeclaredMethod(mname);
        int idx = 0;
        String nname = null;
        while (true) {
            nname = mname + "$impl" + (idx++);
            try {
                clas.getDeclaredMethod(nname);
            } catch (NotFoundException ee) {
                break;
            }
        }
        mold.setName(nname);
        CtMethod mnew = CtNewMethod.copy(mold, mname, clas, null);
        String type = mold.getReturnType().getName();

        StringBuffer body = new StringBuffer();
        body.append("{\n");
        if (!"void".equals(type))
            body.append(type +" result=null;\n");
        body.append("  try{\n");
        if (!"void".equals(type)) {
            body.append(" result= ");
        }
        body.append(nname + "($$);\n");
        body.append("  }finally{\n");
        body.append(code);
        body.append("  }\n");
        if (!"void".equals(type))
            body.append("return result;\n");
        body.append("}");

        mnew.setBody(body.toString());
        clas.addMethod(mnew);
    }

    public void splitData() throws Exception {
        String dataPath = dataDir.getAbsolutePath();
        String[] args = new String[5];
        args[0] = schemaFile.getAbsolutePath();
        args[1] = "--conf";
        args[2] = confDir.getAbsolutePath();
        args[3] = "--output";
        args[4] = dataPath;
        HeapFileSplitter.main(args);
        // int worker0Port = baseServerPort+3*numOfRun+1;
        // int worker1Port = baseServerPort+3*numOfRun+2;
        File f = new File(dataPath + "/localhost_" + workers[0].getPort());
        ParallelUtility.deleteFileFolder(new File(dataPath + "/"
                + workers[0].getPort()));
        f.renameTo(new File(dataPath + "/" + workers[0].getPort()));
        f = new File(dataDir.getAbsolutePath() + "/localhost_"
                + workers[1].getPort());
        ParallelUtility.deleteFileFolder(new File(dataPath + "/"
                + workers[1].getPort()));
        f.renameTo(new File(dataPath + "/" + workers[1].getPort()));
    }

    public File testDir = null;
    public File confDir = null;
    public File dataDir = null;
    public File binDir = null;
    public File libDir = null;
    public File schemaFile = null;

    protected int port = 14444;
    protected SocketInfo server = null;
    protected SocketInfo[] workers = null;

    private int[] findFreePorts() {
        int[] ports = new int[3];
        File f = null;
        ServerSocket ss = null;
        for (int i = 0; i < ports.length; i++) {
            while (true) {
                port++;
                try {
                    f = new File(System.getProperty("java.io.tmpdir")
                            + "/simpledb_bind_ports." + port);
                    if (!f.isFile()) {
                        f.createNewFile();
                        ss = new ServerSocket();
                        ss.setReuseAddress(true);

                        ss.bind(new InetSocketAddress("localhost", port));
                        ss.close();
                        ports[i] = port;
                        break;
                    }
                } catch (Exception ee) {
                    while (f != null && f.isFile() && !f.delete()) {
                    }
                    if (ss != null)
                        try {
                            ss.close();
                        } catch (Exception eee) {
                        }
                }
            }
        }
        return ports;
    }

    private void freeAPort(int port) {
        ServerSocket ss = null;
        while (true) {
            try {
                ss = new ServerSocket();
                ss.setReuseAddress(true);
                ss.bind(new InetSocketAddress("localhost", port));
                ss.close();
                break;
            } catch (IOException e) {
            } finally {
                if (ss != null)
                    try {
                        ss.close();
                    } catch (Exception eee) {
                    }
            }
        }
        File f = new File(System.getProperty("java.io.tmpdir")
                + "/simpledb_bind_ports." + port);
        while (f.isFile() && !f.delete()) {
        }
    }

    @After
    public void clean() {

        freeAPort(server.getPort());
        freeAPort(workers[0].getPort());
        freeAPort(workers[1].getPort());
        if (testDir.exists())
        {
            try {
                ParallelUtility.deleteFileFolder(testDir);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Before
    public void init() throws Exception {
        int[] ports = findFreePorts();
        server = new SocketInfo("localhost", ports[0]);
        workers = new SocketInfo[] { new SocketInfo("localhost", ports[1]),
                new SocketInfo("localhost", ports[2]) };

        Database.reset();
        TableStats.getStatsMap().clear();
        newDatabase();
        String binPath = simpledb.parallel.Server.class.getProtectionDomain()
                .getCodeSource().getLocation().getPath();
        String libPath = new File(org.apache.mina.core.service.IoAcceptor.class
                .getProtectionDomain().getCodeSource().getLocation().getPath())
                .getParent();

        while (true) {
            long randFolder = (long) (Math.random() * Long.MAX_VALUE);
            testDir = new File(System.getProperty("java.io.tmpdir")
                    + "/simpledb_test_" + randFolder);
            if (!testDir.exists())
                break;
        }

        Assert.assertTrue(testDir.mkdirs());
        confDir = new File(testDir.getAbsolutePath() + "/conf");
        dataDir = new File(testDir.getAbsolutePath() + "/data");
        binDir = new File(testDir.getAbsoluteFile() + "/bin");
        libDir = new File(testDir.getAbsoluteFile() + "/lib");

        Assert.assertTrue(confDir.mkdirs());
        Assert.assertTrue(dataDir.mkdirs());
        Assert.assertTrue(binDir.mkdirs());

        String dataPath = dataDir.getAbsolutePath();
        ParallelUtility.writeFile(new File(confDir.getAbsolutePath()
                + "/server.conf"), "localhost:" + server.getPort() + "\n");
        ParallelUtility.writeFile(new File(confDir.getAbsoluteFile()
                + "/workers.conf"), "localhost:" + workers[0].getPort()
                + "\nlocalhost:" + workers[1].getPort() + "\n");
        schemaFile = new File(dataPath + "/test.schema");
        ParallelUtility.writeFile(schemaFile, schema);
        File studentCSVF = new File(dataPath + "/student.csv");
        File advisorCSVF = new File(dataPath + "/advisor.csv");
        ParallelUtility.writeFile(studentCSVF, studentCSV);
        ParallelUtility.writeFile(advisorCSVF, advisorCSV);
        HeapFileEncoder.convert(studentCSVF,
                new File(dataPath + "/student.dat"), BufferPool.PAGE_SIZE, 3,
                new Type[] { Type.INT_TYPE, Type.STRING_TYPE, Type.INT_TYPE });
        HeapFileEncoder.convert(advisorCSVF,
                new File(dataPath + "/advisor.dat"), BufferPool.PAGE_SIZE, 2,
                new Type[] { Type.INT_TYPE, Type.STRING_TYPE });
        splitData();
        ParallelUtility.copyFileFolder(new File(binPath), binDir, true);
        ParallelUtility.copyFileFolder(new File(libPath), libDir, true);
    }

    public String schema;
    public String advisorCSV;
    public String studentCSV;

    public void newDatabase() {
        StringBuilder sb = new StringBuilder();
        sb.append("advisor(id int pk, name string)\n");
        sb.append("student(id int pk, name string, aid int)\n");
        schema = sb.toString();
        System.out.println(schema);
        String[] advisorNames = { "Sun", "Mercury", "Venus", "Earth", "Mars",
                "Jupiter", "Saturn", "Uranus", "Neptune" };
        StringBuilder aSB = new StringBuilder();
        int idx = 0;
        for (int i = 0; i < advisorNames.length; i++) {// 18 advisors
            aSB.append((idx++) + "," + advisorNames[i] + "\n");
            aSB.append((idx++) + "," + advisorNames[i] + "\n");
        }
        advisorCSV = aSB.toString();
        int numAdvisor = idx;
        String[] studentNames = { "Turing", "Einstein", "Confucius", "Plato",
                "Newton", "Euler", "Shannon" };
        idx = 0;
        StringBuilder sSB = new StringBuilder();
        for (int i = 0; i < studentNames.length; i++) {// 700 students
            for (int j = 0; j < 100; j++)
                sSB.append((idx++) + "," + studentNames[i] + ","
                        + (int) (Math.random() * numAdvisor) + "\n");
        }
        studentCSV = sSB.toString();
    }
}
