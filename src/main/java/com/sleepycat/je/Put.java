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

package com.sleepycat.je;

import com.sleepycat.je.dbi.PutMode;

/** The operation type passed to "put" methods on databases and cursors. */
public enum Put {

  OVERWRITE(PutMode.OVERWRITE),
  NO_OVERWRITE(PutMode.NO_OVERWRITE),
  DUP_DATA(PutMode.DUP_DATA),
  CURRENT(PutMode.CURRENT);

  private final PutMode putMode;

  Put(final PutMode putMode) {
    this.putMode = putMode;
  }

  PutMode getPutMode() {
    return putMode;
  }
}
