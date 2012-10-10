package edu.washington.escience.myriad.datalog.parser;

import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.io.FileReader;

import org.junit.Test;

import edu.washington.escience.myriad.datalog.syntax.DatalogProgram;

public class DatalogParserTest {

  @Test
  public void test() {
    final String filename = "test/datalog/paths.dlog";
    FileReader reader;
    try {
      reader = new FileReader(filename);
    } catch (FileNotFoundException e) {
      fail(e.toString());
      return;
    }

    DatalogParser parser = new DatalogParser(reader);
    DatalogProgram program;
    try {
      program = parser.Program();
      System.out.println(program.toString());
    } catch (ParseException e) {
      fail(e.toString());
    }
  }
}