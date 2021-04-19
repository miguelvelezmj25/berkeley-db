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

package com.sleepycat.je.jca.ra;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.XAEnvironment;

import javax.resource.ResourceException;
import javax.resource.spi.ConnectionEvent;

public class JELocalTransaction
    implements javax.resource.cci.LocalTransaction, javax.resource.spi.LocalTransaction {

  private static final boolean DEBUG = false;

  private final transient XAEnvironment env;
  private final transient TransactionConfig transConfig;
  private final transient JEManagedConnection mgdConn;

  JELocalTransaction(
      XAEnvironment env, TransactionConfig transConfig, JEManagedConnection mgdConn) {
    this.env = env;
    this.transConfig = transConfig;
    this.mgdConn = mgdConn;
  }

  public Transaction getTransaction() throws DatabaseException {

    return env.getThreadTransaction();
  }

  protected XAEnvironment getEnv() {
    return env;
  }

  private void checkEnv(String methodName) throws ResourceException {

    if (env == null) {
      throw new ResourceException("env is null in " + methodName);
    }
  }

  /*
   * Methods for LocalTransaction.
   */

  public void begin() throws ResourceException {

    checkEnv("begin");
    long id = -1;
    try {
      Transaction txn = env.beginTransaction(null, transConfig);
      env.setThreadTransaction(txn);
      id = txn.getId();
    } catch (DatabaseException DE) {
      throw new ResourceException("During begin: " + DE);
    }

    ConnectionEvent connEvent =
        new ConnectionEvent(mgdConn, ConnectionEvent.LOCAL_TRANSACTION_STARTED);
    connEvent.setConnectionHandle(mgdConn);
    mgdConn.sendConnectionEvent(connEvent);

    if (DEBUG) {
      System.out.println("JELocalTransaction.begin " + id);
    }
  }

  public void commit() throws ResourceException {

    checkEnv("commit");
    try {
      env.getThreadTransaction().commit();
    } catch (DatabaseException DE) {
      ResourceException ret = new ResourceException(DE.toString(), DE);
        throw ret;
    } finally {
      env.setThreadTransaction(null);
    }

    ConnectionEvent connEvent =
        new ConnectionEvent(mgdConn, ConnectionEvent.LOCAL_TRANSACTION_COMMITTED);
    connEvent.setConnectionHandle(mgdConn);
    mgdConn.sendConnectionEvent(connEvent);

    if (DEBUG) {
      System.out.println("JELocalTransaction.commit");
    }
  }

  public void rollback() throws ResourceException {

    checkEnv("rollback");
    try {
      env.getThreadTransaction().abort();
    } catch (DatabaseException DE) {
      ResourceException ret = new ResourceException(DE.toString(), DE);
        throw ret;
    } finally {
      env.setThreadTransaction(null);
    }

    ConnectionEvent connEvent =
        new ConnectionEvent(mgdConn, ConnectionEvent.LOCAL_TRANSACTION_ROLLEDBACK);
    connEvent.setConnectionHandle(mgdConn);
    mgdConn.sendConnectionEvent(connEvent);

    if (DEBUG) {
      System.out.println("JELocalTransaction.rollback");
    }
  }
}
