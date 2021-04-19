/*-
 * Copyright (C) 2002, 2017, Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle Berkeley
 * DB Java Edition made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/berkeleydb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle Berkeley DB Java Edition for a copy of the
 * license and additional information.
 */

package com.sleepycat.je.txn;

import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.Loggable;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.Timestamp;
import com.sleepycat.je.utilint.VLSN;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * This class indicates the end of a partial rollback at syncup. This is a non-replicated entry.
 * Although this is a replication related class, it resides in the utilint package because it is
 * referenced in LogEntryType.java, and is used in a general way at recovery.
 */
public class RollbackStart implements Loggable {

  /* The matchpoint that is the logical start of this rollback period. */
  private VLSN matchpointVLSN;
  private long matchpointLSN;

  /*
   * The active txn list are unfinished transactions that will be rolled back
   * by syncup.
   */
  private Set<Long> activeTxnIds;

  /* For debugging in the field */
  private Timestamp time;

  public RollbackStart(VLSN matchpointVLSN, long matchpointLSN, Set<Long> activeTxnIds) {
    this.matchpointVLSN = matchpointVLSN;
    this.matchpointLSN = matchpointLSN;
    this.activeTxnIds = activeTxnIds;
    time = new Timestamp(System.currentTimeMillis());
  }

  /** For constructing from the log. */
  public RollbackStart() {}

  public long getMatchpoint() {
    return matchpointLSN;
  }

  public Set<Long> getActiveTxnIds() {
    return activeTxnIds;
  }

  public VLSN getMatchpointVLSN() {
    return matchpointVLSN;
  }

  /** @see Loggable#getLogSize */
  public int getLogSize() {
    int size =
        LogUtils.getPackedLongLogSize(matchpointVLSN.getSequence())
            + LogUtils.getPackedLongLogSize(matchpointLSN)
            + LogUtils.getTimestampLogSize(time)
            + LogUtils.getPackedIntLogSize(activeTxnIds.size());

    for (Long id : activeTxnIds) {
      size += LogUtils.getPackedLongLogSize(id);
    }

    return size;
  }

  /** @see Loggable#writeToLog */
  public void writeToLog(ByteBuffer buffer) {
    LogUtils.writePackedLong(buffer, matchpointVLSN.getSequence());
    LogUtils.writePackedLong(buffer, matchpointLSN);
    LogUtils.writeTimestamp(buffer, time);
    LogUtils.writePackedInt(buffer, activeTxnIds.size());
    for (Long id : activeTxnIds) {
      LogUtils.writePackedLong(buffer, id);
    }
  }

  /**
   * "
   *
   * @see Loggable#readFromLog
   */
  public void readFromLog(ByteBuffer buffer, int entryVersion) {
    matchpointVLSN = new VLSN(LogUtils.readPackedLong(buffer));
    matchpointLSN = LogUtils.readPackedLong(buffer);
    /* the timestamp is packed -- double negative, unpacked == false */
    time = LogUtils.readTimestamp(buffer, false /* unpacked. */);
    int setSize = LogUtils.readPackedInt(buffer);
    activeTxnIds = new HashSet<Long>(setSize);
    for (int i = 0; i < setSize; i++) {
      activeTxnIds.add(LogUtils.readPackedLong(buffer));
    }
  }

  /** @see Loggable#dumpLog */
  public void dumpLog(StringBuilder sb, boolean verbose) {
    sb.append(" matchpointVLSN=").append(matchpointVLSN.getSequence());
    sb.append(" matchpointLSN=");
    sb.append(DbLsn.getNoFormatString(matchpointLSN));

    /* Make sure the active txns are listed in order, partially for the sake
     * of the LoggableTest unit test, which expects the toString() for two
     * equivalent objects to always display the same, and partially for
     * ease of debugging.
     */
    List<Long> displayTxnIds = new ArrayList<Long>(activeTxnIds);
    Collections.sort(displayTxnIds);
    sb.append(" activeTxnIds=").append(displayTxnIds);
    sb.append("\" time=\"").append(time);
  }

  /** @see Loggable#getTransactionId */
  public long getTransactionId() {
    return 0;
  }

  /** @see Loggable#logicalEquals */
  public boolean logicalEquals(Loggable other) {

    if (!(other instanceof RollbackStart)) {
      return false;
    }

    RollbackStart otherRS = (RollbackStart) other;

    return (matchpointVLSN.equals(otherRS.matchpointVLSN)
        && (matchpointLSN == otherRS.matchpointLSN)
        && time.equals(otherRS.time)
        && activeTxnIds.equals(otherRS.activeTxnIds));
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    dumpLog(sb, true);
    return sb.toString();
  }
}
