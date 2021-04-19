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

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.MemoryBudget;

import java.util.List;
import java.util.Set;

/** A Lock embodies the lock state of an LSN. It includes a set of owners and a list of waiters. */
interface Lock {

  /** Get a list of waiters for debugging and error messages. */
  List<LockInfo> getWaitersListClone();

  /** Remove this locker from the waiter list. */
  void flushWaiter(Locker locker, MemoryBudget mb, int lockTableIndex);

  /** Get a new Set of the owners. */
  Set<LockInfo> getOwnersClone();

  /**
   * Return true if locker is an owner of this Lock for lockType, false otherwise.
   *
   * <p>This method is only used by unit tests.
   */
  boolean isOwner(Locker locker, LockType lockType);

  /** Return true if locker is an owner of this Lock and this is a write lock. */
  boolean isOwnedWriteLock(Locker locker);

  /** Returns the LockType if the given locker owns this lock, or null if the lock is not owned. */
  LockType getOwnedLockType(Locker locker);

  /**
   * Return true if locker is a waiter on this Lock.
   *
   * <p>This method is only used by unit tests.
   */
  boolean isWaiter(Locker locker);

  int nWaiters();

  int nOwners();

  /**
   * Attempts to acquire the lock and returns the LockGrantType.
   *
   * <p>Assumes we hold the lockTableLatch when entering this method.
   */
  LockAttemptResult lock(
          LockType requestType,
          Locker locker,
          boolean nonBlockingRequest,
          boolean jumpAheadOfWaiters,
          MemoryBudget mb,
          int lockTableIndex)
      throws DatabaseException;

  /**
   * Releases a lock and moves the next waiter(s) to the owners.
   *
   * @return - null if we were not the owner, - a non-empty set if owners should be notified after
   *     releasing, - an empty set if no notification is required.
   */
  Set<Locker> release(Locker locker, MemoryBudget mb, int lockTableIndex);

  /**
   * Removes all owners except for the given owner, and sets the Preempted property on the removed
   * owners.
   */
  void stealLock(Locker locker, MemoryBudget mb, int lockTableIndex)
      throws DatabaseException;

  /** Downgrade a write lock to a read lock. */
  void demote(Locker locker);

  /**
   * Return the locker that has a write ownership on this lock. If no write owner exists, return
   * null.
   */
  Locker getWriteOwnerLocker();

  boolean isThin();

  /** Debug dumper. */
  String toString();
}
