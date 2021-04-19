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

package com.sleepycat.je.rep.impl.node;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.rep.utilint.BinaryProtocol.Message;
import com.sleepycat.je.utilint.StatGroup;

import static com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition.N_BYTES_READ;
import static com.sleepycat.je.rep.utilint.BinaryProtocolStatDefinition.N_MESSAGES_READ;

public class ReplicaFactory {
  private static ReplicaType type = ReplicaType.DEFAULT;
  private static long statsInterval = 0;

  public enum ReplicaType {
    DEFAULT,
    NULL_REPLICA
  }

  public static void setReplicaType(ReplicaType t) {
    type = t;
  }

  public static void setStatsInterval(long interval) {
    statsInterval = interval;
  }

  public static Replica create(RepNode repNode, Replay replay) {
    switch (type) {
      case DEFAULT:
        return new Replica(repNode, replay);

      case NULL_REPLICA:

        /** Create a replica which just eats messages. Used for testing network bandwidth. */
        return new Replica(repNode, replay) {
          @Override
          protected void doRunReplicaLoopInternalWork() throws Exception {

            long ctime = System.currentTimeMillis();
            long opCount = 0;
            while (true) {
              opCount++;
              @SuppressWarnings("unused")
              Message message = getProtocol().read(getReplicaFeederChannel());
              if (statsInterval > 0 && (opCount % statsInterval) == 0) {
                StatGroup stats = getProtocol().getStats(StatsConfig.DEFAULT);
                long bytesRead = stats.getLong(N_BYTES_READ);
                long messagesRead = stats.getLong(N_MESSAGES_READ);
                long elapsedTime = System.currentTimeMillis() - ctime;
                long bytesPerMilliSecond = bytesRead / elapsedTime;
                System.out.println(
                    " Bytes Read: "
                        + bytesRead
                        + " Messages Read: "
                        + messagesRead
                        + " BytesPerMSec: "
                        + bytesPerMilliSecond
                        + " MS: "
                        + elapsedTime);
                ctime = System.currentTimeMillis();
              }
            }
          }
        };

      default:
        throw EnvironmentFailureException.unexpectedState(
            "unknown type passed to makeReplica: " + type);
    }
  }
}
