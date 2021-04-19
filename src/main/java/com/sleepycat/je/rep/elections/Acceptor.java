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

package com.sleepycat.je.rep.elections;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.JEVersion;
import com.sleepycat.je.rep.elections.Acceptor.SuggestionGenerator.Ranking;
import com.sleepycat.je.rep.elections.Proposer.Proposal;
import com.sleepycat.je.rep.elections.Protocol.Accept;
import com.sleepycat.je.rep.elections.Protocol.Propose;
import com.sleepycat.je.rep.elections.Protocol.Value;
import com.sleepycat.je.rep.impl.TextProtocol.InvalidMessageException;
import com.sleepycat.je.rep.impl.TextProtocol.RequestMessage;
import com.sleepycat.je.rep.impl.TextProtocol.ResponseMessage;
import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.utilint.ServiceDispatcher;
import com.sleepycat.je.utilint.LoggerUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.channels.Channels;
import java.util.logging.Level;

/**
 * Plays the role of Acceptor in the consensus algorithm. It runs in its own thread listening for
 * and responding to messages sent by Proposers.
 */
public class Acceptor extends ElectionAgentThread {

  /*
   * The currently promised proposal. Proposals below this one are rejected.
   */
  private Proposal promisedProposal = null;

  private Value acceptedValue = null;

  /* Used to return suggestions in response to Propose requests. */
  private final SuggestionGenerator suggestionGenerator;

  /* Identifies the Acceptor Service. */
  public static final String SERVICE_NAME = "Acceptor";

  private final ElectionsConfig config;

  /** Creates an Acceptor */
  public Acceptor(
      Protocol protocol, ElectionsConfig config, SuggestionGenerator suggestionGenerator) {

    super(config.getRepImpl(), protocol, "Acceptor Thread " + config.getNameIdPair().getName());
    this.config = config;

    this.suggestionGenerator = suggestionGenerator;
  }

  /** The Acceptor thread body. */
  @Override
  public void run() {
    final ServiceDispatcher serviceDispatcher = config.getServiceDispatcher();
    serviceDispatcher.register(SERVICE_NAME, channelQueue);
    LoggerUtils.logMsg(logger, envImpl, formatter, Level.FINE, "Acceptor started");
    DataChannel channel = null;
    try {
      while (true) {
        channel =
            serviceDispatcher.takeChannel(
                SERVICE_NAME, true /* block */, protocol.getReadTimeout());

        if (channel == null) {
          /* A soft shutdown. */
          return;
        }

        BufferedReader in = null;
        PrintWriter out = null;
        try {
          in = new BufferedReader(new InputStreamReader(Channels.newInputStream(channel)));
          out = new PrintWriter(Channels.newOutputStream(channel), true);
          String requestLine = in.readLine();
          if (requestLine == null) {
            LoggerUtils.logMsg(logger, envImpl, formatter, Level.FINE, "Acceptor: EOF on request");
            continue;
          }
          RequestMessage requestMessage = null;
          try {
            requestMessage = protocol.parseRequest(requestLine);
          } catch (InvalidMessageException ime) {
            protocol.processIME(channel, ime);
            continue;
          }
          ResponseMessage responseMessage = null;
          if (requestMessage.getOp() == protocol.PROPOSE) {
            responseMessage = process((Propose) requestMessage);
          } else if (requestMessage.getOp() == protocol.ACCEPT) {
            responseMessage = process((Accept) requestMessage);
          } else if (requestMessage.getOp() == protocol.SHUTDOWN) {
            break;
          } else {
            LoggerUtils.logMsg(
                logger, envImpl, formatter, Level.SEVERE, "Unrecognized request: " + requestLine);
            continue;
          }

          /*
           * The request message may be of an earlier version. If so,
           * this node transparently read the older version. JE only
           * throws out InvalidMesageException when the version of
           * the request message is newer than the current protocol.
           * To avoid sending a repsonse that the requester cannot
           * understand, we send a response in the same version as
           * that of the original request message.
           */
          responseMessage.setSendVersion(requestMessage.getSendVersion());
          out.println(responseMessage.wireFormat());
        } catch (IOException e) {
          LoggerUtils.logMsg(
              logger, envImpl, formatter, Level.WARNING, "IO error on socket: " + e.getMessage());
          continue;
        } finally {
          Utils.cleanup(logger, envImpl, formatter, channel, in, out);
          cleanup();
        }
      }
    } catch (InterruptedException e) {
      if (isShutdown()) {
        /* Treat it like a shutdown, exit the thread. */
        return;
      }
      LoggerUtils.logMsg(
          logger, envImpl, formatter, Level.WARNING, "Acceptor unexpected interrupted");
      throw EnvironmentFailureException.unexpectedException(e);
    } finally {
      serviceDispatcher.cancel(SERVICE_NAME);
      cleanup();
    }
  }

  /**
   * Responds to a Propose request.
   *
   * @param propose the request proposal
   * @return the response: a Promise if the request was accepted, a Reject otherwise.
   */
  ResponseMessage process(Propose propose) {

    if ((promisedProposal != null) && (promisedProposal.compareTo(propose.getProposal()) > 0)) {
      LoggerUtils.logMsg(
          logger,
          envImpl,
          formatter,
          Level.FINE,
          "Reject Propose: " + propose.getProposal() + " Promised proposal: " + promisedProposal);
      return protocol.new Reject(promisedProposal);
    }

    promisedProposal = propose.getProposal();
    final Value suggestedValue = suggestionGenerator.get(promisedProposal);
    final Ranking suggestionRanking = suggestionGenerator.getRanking(promisedProposal);
    LoggerUtils.logMsg(
        logger,
        envImpl,
        formatter,
        Level.FINE,
        "Promised: "
            + promisedProposal
            + " Suggested Value: "
            + suggestedValue
            + " Suggestion Ranking: "
            + suggestionRanking);
    return protocol
    .new Promise(
        promisedProposal,
        acceptedValue,
        suggestedValue,
        suggestionRanking,
        config.getElectionPriority(),
        config.getLogVersion(),
        JEVersion.CURRENT_VERSION);
  }

  /**
   * Responds to Accept request
   *
   * @param accept the request
   * @return an Accepted or Reject response as appropriate.
   */
  ResponseMessage process(Accept accept) {
    if ((promisedProposal != null) && (promisedProposal.compareTo(accept.getProposal()) != 0)) {
      LoggerUtils.logMsg(
          logger,
          envImpl,
          formatter,
          Level.FINE,
          "Reject Accept: " + accept.getProposal() + " Promised proposal: " + promisedProposal);
      return protocol.new Reject(promisedProposal);
    }
    acceptedValue = accept.getValue();
    LoggerUtils.logMsg(
        logger,
        envImpl,
        formatter,
        Level.FINE,
        "Promised: "
            + promisedProposal
            + " Accepted: "
            + accept.getProposal()
            + " Value: "
            + acceptedValue);
    return protocol.new Accepted(accept.getProposal(), acceptedValue);
  }

  public interface SuggestionGenerator {

    /**
     * Used to generate a suggested value for use by a Proposer. It's a hint. The proposal argument
     * may be used to freeze values like the VLSN number from advancing (if they were used in the
     * ranking) until an election has completed.
     *
     * @param proposal the Proposal for which the value is being suggested.
     * @return the suggested value.
     */
    Value get(Proposal proposal);

    /**
     * The importance associated with the above suggestion. Acceptors have to agree on a common
     * system for ranking importance so that the relative importance of different suggestions can be
     * meaningfully compared.
     *
     * @param the proposal associated with the ranking
     * @return the importance of the suggestion as a number
     */
    Ranking getRanking(Proposal proposal);

    /** A description of the ranking used when comparing Promises to pick a Master. */
    class Ranking implements Comparable<Ranking> {
      /* The major component of the ranking. */
      final long major;

      /* The minor component. */
      final long minor;

      static Ranking UNINITIALIZED = new Ranking(Long.MIN_VALUE, Long.MIN_VALUE);

      public Ranking(long major, long minor) {
        this.major = major;
        this.minor = minor;
      }

      @Override
      public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (major ^ (major >>> 32));
        result = prime * result + (int) (minor ^ (minor >>> 32));
        return result;
      }

      @Override
      public boolean equals(Object obj) {
        if (this == obj) {
          return true;
        }
        if (obj == null) {
          return false;
        }
        if (getClass() != obj.getClass()) {
          return false;
        }
        Ranking other = (Ranking) obj;
        if (major != other.major) {
          return false;
        }
          return minor == other.minor;
      }

      @Override
      public String toString() {
        return "Ranking major:" + major + " minor:" + minor;
      }

      @Override
      public int compareTo(Ranking o) {
        int result = Long.compare(major, o.major);
        if (result != 0) {
          return result;
        }
        return Long.compare(minor, o.minor);
      }
    }
  }
}
