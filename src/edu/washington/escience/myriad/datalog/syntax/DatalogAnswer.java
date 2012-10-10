package edu.washington.escience.myriad.datalog.syntax;

public class DatalogAnswer {
  private DatalogAtom answer = null;

  public DatalogAnswer(final DatalogAtom a) {
    answer = a;
  }

  public DatalogAtom getAnswer() {
    return answer;
  }

  @Override
  public String toString() {
    String res = "answer: ";
    if (answer != null) {
      res = res + answer.toString();
    }
    return res;
  }
}