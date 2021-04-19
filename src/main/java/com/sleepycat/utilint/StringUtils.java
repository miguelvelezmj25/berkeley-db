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

package com.sleepycat.utilint;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.*;

public class StringUtils {

  private static final Charset ASCII = StandardCharsets.US_ASCII;
  private static final Charset UTF8 = StandardCharsets.UTF_8;

  /**
   * In all src and test code, the String(byte[], ...) constructor and String.getBytes method must
   * always be passed a Charset, to avoid portability issues. Otherwise, portability issues will
   * occur when running on a JVM plataform with a non-western default charset, the EBCDIC encoding
   * (on z/OS), etc. [#20967]
   *
   * <p>In most cases, the UTF8 or ASCII charset should be used for portability. UTF8 should be used
   * when any character may be represented. ASCII can be used when all characters are in the ASCII
   * range. The default charset should only be used when handling user-input data directly, e.g.,
   * console input/output or user-visible files.
   *
   * <p>Rather than passing the charset as a string (getBytes("UTF-8")), the Charset objects defined
   * here should be passed (getBytes(UTF8)). Not only is using a Charset object slightly more
   * efficient because it avoids a lookup, even more importantly it avoids having to clutter code
   * with a catch for java.io.UnsupportedEncodingException, which should never be thrown for the
   * "UTF-8" or "US-ASCII" charsets.
   */
  public static byte[] toUTF8(String str) {
      return str.getBytes(StandardCharsets.UTF_8);
  }

  /** @return a buffer with position set to 0 */
  public static ByteBuffer toUTF8(CharBuffer chars) {
    try {
      final CharsetEncoder utf8Encoder = UTF8.newEncoder();
      return utf8Encoder.encode(chars);
    } catch (CharacterCodingException e) {
      // Should never happen.
      throw new RuntimeException(e);
    }
  }

  public static String fromUTF8(byte[] bytes) {
      return new String(bytes, StandardCharsets.UTF_8);
  }

  public static String fromUTF8(byte[] bytes, int offset, int len) {
      return new String(bytes, offset, len, StandardCharsets.UTF_8);
  }

  /** @return a buffer with position set to 0 */
  public static CharBuffer fromUTF8(ByteBuffer bytes) {
    try {
      final CharsetDecoder utf8Decoder = UTF8.newDecoder();
      return utf8Decoder.decode(bytes);
    } catch (CharacterCodingException e) {
      // Should never happen.
      throw new RuntimeException(e);
    }
  }

  public static byte[] toASCII(String str) {
      return str.getBytes(StandardCharsets.US_ASCII);
  }

  /** @return a buffer with position set to 0 */
  public static ByteBuffer toASCII(CharBuffer chars) {
    try {
      final CharsetEncoder asciiEncoder = ASCII.newEncoder();
      return asciiEncoder.encode(chars);
    } catch (CharacterCodingException e) {
      // Should never happen.
      throw new RuntimeException(e);
    }
  }

  public static String fromASCII(byte[] bytes) {
      return new String(bytes, StandardCharsets.US_ASCII);
  }

  public static String fromASCII(byte[] bytes, int offset, int len) {
      return new String(bytes, offset, len, StandardCharsets.US_ASCII);
  }

  /** @return a buffer with position set to 0 */
  public static CharBuffer fromASCII(ByteBuffer bytes) {
    try {
      final CharsetDecoder asciiDecoder = ASCII.newDecoder();
      return asciiDecoder.decode(bytes);
    } catch (CharacterCodingException e) {
      // Should never happen.
      throw new RuntimeException(e);
    }
  }
}
