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

package com.sleepycat.je.rep.stream;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.utilint.VLSN;

import java.io.IOException;

/**
 * Provides the next log record, blocking if one is not available. It encapsulates the source of the
 * Log records, which can be a real Master or a Replica in a Replica chain that is replaying log
 * records it received from some other source.
 */
public interface FeederSource {

  void shutdown(EnvironmentImpl envImpl);

  /** Must be called to allow deletion of files protected by this feeder. */
  OutputWireRecord getWireRecord(VLSN vlsn, int waitTime)
      throws DatabaseException, InterruptedException, IOException;

  String dumpState();
}
