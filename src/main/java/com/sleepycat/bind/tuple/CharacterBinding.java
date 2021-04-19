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

package com.sleepycat.bind.tuple;

import com.sleepycat.je.DatabaseEntry;

/**
 * A concrete <code>TupleBinding</code> for a <code>Character</code> primitive wrapper or a <code>
 * char</code> primitive.
 *
 * <p>There are two ways to use this class:
 *
 * <ol>
 *   <li>When using the {@link com.sleepycat.je} package directly, the static methods in this class
 *       can be used to convert between primitive values and {@link DatabaseEntry} objects.
 *   <li>When using the {@link com.sleepycat.collections} package, an instance of this class can be
 *       used with any stored collection. The easiest way to obtain a binding instance is with the
 *       {@link TupleBinding#getPrimitiveBinding} method.
 * </ol>
 *
 * @see <a href="package-summary.html#integerFormats">Integer Formats</a>
 */
public class CharacterBinding extends TupleBinding<Character> {

  private static final int CHAR_SIZE = 2;

  // javadoc is inherited
  public Character entryToObject(TupleInput input) {

    return input.readChar();
  }

  // javadoc is inherited
  public void objectToEntry(Character object, TupleOutput output) {

    output.writeChar(object);
  }

  // javadoc is inherited
  protected TupleOutput getTupleOutput(Character object) {

    return sizedOutput();
  }

  /**
   * Converts an entry buffer into a simple <code>char</code> value.
   *
   * @param entry is the source entry buffer.
   * @return the resulting value.
   */
  public static char entryToChar(DatabaseEntry entry) {

    return entryToInput(entry).readChar();
  }

  /**
   * Converts a simple <code>char</code> value into an entry buffer.
   *
   * @param val is the source value.
   * @param entry is the destination entry buffer.
   */
  public static void charToEntry(char val, DatabaseEntry entry) {

    outputToEntry(sizedOutput().writeChar(val), entry);
  }

  /**
   * Returns a tuple output object of the exact size needed, to avoid wasting space when a single
   * primitive is output.
   */
  private static TupleOutput sizedOutput() {

    return new TupleOutput(new byte[CHAR_SIZE]);
  }
}
