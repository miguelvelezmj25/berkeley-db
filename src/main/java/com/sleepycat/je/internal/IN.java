package com.sleepycat.je.internal;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.dbi.ExpirationInfo;
import com.sleepycat.je.dbi.PutMode;
import com.sleepycat.je.txn.Locker;

public class IN {

  /**
   * Internal call;
   */
  public static OperationResult putHandleDups(
      DatabaseEntry key,
      DatabaseEntry data,
      CacheMode cacheMode,
      ExpirationInfo expInfo,
      PutMode putMode) {
    return null;
  }

  /**
   * Internal call;
   */
  public static OperationResult putHandleDupsSync(
      DatabaseEntry key,
      DatabaseEntry data,
      CacheMode cacheMode,
      ExpirationInfo expInfo,
      PutMode putMode,
      Locker locker) {
    return null;
  }

  /**
   * Internal call;
   */
  public static OperationResult putNoDups(
      DatabaseEntry key,
      DatabaseEntry data,
      CacheMode cacheMode,
      ExpirationInfo expInfo,
      PutMode putMode) {
    return null;
  }
}
