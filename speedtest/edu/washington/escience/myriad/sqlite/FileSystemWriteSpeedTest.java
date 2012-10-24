package edu.washington.escience.myriad.sqlite;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Date;
import java.util.Random;

import org.junit.Test;

public class FileSystemWriteSpeedTest {

  @Test
  public static void filesystemWriteTest() throws Exception {
    final Date now = new Date();
    final Date begin = now;
    final Random r = new Random();
    final File f = new File("/tmp/tmpfile");
    final FileOutputStream fos = new FileOutputStream(f);

    for (int i = 0; i < 1000000; i++) {
      fos.write((i + "|" + i + "th " + r.nextInt()).getBytes());
    }
    fos.close();
    System.out.println((new Date().getTime() - begin.getTime()) * 1.0 / 1000 + " seconds in total");
    // 2.371 seconds
  }
}
