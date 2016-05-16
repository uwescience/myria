package edu.washington.escience.myria.sqlite;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Date;
import java.util.Random;

import org.junit.Test;

public class FileSystemWriteSpeedTest {

  private final int NUM_TUPLES = 1000 * 1000;

  @Test
  public void filesystemWriteTest() throws Exception {
    System.out.println("Filesystem write test:");

    final Random r = new Random();
    final File f = new File("/tmp/tmpfile");
    final String[] strings = new String[NUM_TUPLES];

    /* Compute the strings first, so we just test writing speed. */
    for (int i = 0; i < NUM_TUPLES; i++) {
      strings[i] = i + "|" + i + "th " + r.nextInt();
    }

    /* Write out the precomputed strings. */
    final FileOutputStream fos = new FileOutputStream(f);
    final Date begin = new Date();
    for (int i = 0; i < NUM_TUPLES; i++) {
      fos.write(strings[i].getBytes());
    }
    fos.close();

    /* Print out stats. */
    final double elapsed = (new Date().getTime() - begin.getTime()) / 1000.0;
    System.out.printf(
        "\t%.2f seconds in total (%.0f tuples per second)\n", elapsed, NUM_TUPLES / elapsed);
  }
}
