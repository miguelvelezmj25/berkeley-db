package edu.cmu.cs.mvelezce.optionhotspot.sources;

import com.sleepycat.je.OperationResult;

public class Sources {

  public static boolean getDuplicates(boolean b) {
    return b;
  }

  public static boolean getTransactions(boolean b) {
    return b;
  }

  public static boolean getTemporary(boolean b) {
    return b;
  }

  public static OperationResult expensive1() {
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return null;
  }

  public static OperationResult expensive2() {
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return null;
  }

  public static OperationResult cheap() {
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return null;
  }
}
