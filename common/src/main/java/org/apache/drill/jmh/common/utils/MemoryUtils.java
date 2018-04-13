/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.apache.drill.jmh.common.utils;

import java.lang.reflect.Field;
import java.nio.ByteOrder;

import org.apache.drill.exec.memory.BoundsChecking;

import sun.misc.Unsafe;

/** Exposes advanced Memory Access APIs for Little-Endian / Unaligned platforms */
@SuppressWarnings("restriction")
public final class MemoryUtils {

  // Ensure this is a little-endian hardware */
  static {
    if (ByteOrder.nativeOrder() != ByteOrder.LITTLE_ENDIAN) {
      throw new IllegalStateException("Drill only runs on LittleEndian systems.");
    }
  }

  /** Java's unsafe object */
  private static Unsafe UNSAFE;

  static {
    try {
      Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
      theUnsafe.setAccessible(true);
      UNSAFE = (Unsafe) theUnsafe.get(null);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** Byte arrays offset */
  private static final long BYTE_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

  /** Number of bytes in a long */
  public static final int LONG_NUM_BYTES  = 8;
  /** Number of bytes in an int */
  public static final int INT_NUM_BYTES   = 4;
  /** Number of bytes in a short */
  public static final int SHORT_NUM_BYTES = 2;

//----------------------------------------------------------------------------
// APIs
//----------------------------------------------------------------------------

  /**
   * @param data source byte array
   * @param index index within the byte array
   * @return short value starting at data+index
   */
  public static short getShort(byte[] data, int index) {
    check(index, SHORT_NUM_BYTES, data.length);
    return UNSAFE.getShort(data, BYTE_ARRAY_OFFSET + index);
  }

  /**
   * @param data source byte array
   * @param index index within the byte array
   * @return integer value starting at data+index
   */
  public static int getInt(byte[] data, int index) {
    check(index, INT_NUM_BYTES, data.length);
    return UNSAFE.getInt(data, BYTE_ARRAY_OFFSET + index);
  }

  /**
   * @param data data source byte array
   * @param index index within the byte array
   * @return long value read at data_index
   */
  public static long getLong(byte[] data, int index) {
    check(index, LONG_NUM_BYTES, data.length);
    return UNSAFE.getLong(data, BYTE_ARRAY_OFFSET + index);
  }

  /**
   * Read a short at position src+srcIndex and copy it to the dest+destIndex
   *
   * @param src source byte array
   * @param srcIndex source index
   * @param dest destination byte array
   * @param destIndex destination index
   */
  public static void putShort(byte[] src, int srcIndex, byte[] dest, int destIndex) {
    check(srcIndex, SHORT_NUM_BYTES, src.length);
    check(destIndex,SHORT_NUM_BYTES, dest.length);

    short value = UNSAFE.getShort(src, BYTE_ARRAY_OFFSET + srcIndex);
    UNSAFE.putShort(dest, (BYTE_ARRAY_OFFSET + destIndex), value);
  }

  /**
   * Read an integer at position src+srcIndex and copy it to the dest+destIndex
   *
   * @param src source byte array
   * @param srcIndex source index
   * @param dest destination byte array
   * @param destIndex destination index
   */
  public static void putInt(byte[] src, int srcIndex, byte[] dest, int destIndex) {
    check(srcIndex, INT_NUM_BYTES, src.length);
    check(destIndex,INT_NUM_BYTES, dest.length);

    int value = UNSAFE.getInt(src, BYTE_ARRAY_OFFSET + srcIndex);
    UNSAFE.putInt(dest, (BYTE_ARRAY_OFFSET + destIndex), value);
  }

  /**
   * Read a long at position src+srcIndex and copy it to the dest+destIndex
   *
   * @param src source byte array
   * @param srcIndex source index
   * @param dest destination byte array
   * @param destIndex destination index
   */
  public static void putLong(byte[] src, int srcIndex, byte[] dest, int destIndex) {
    check(srcIndex, LONG_NUM_BYTES, src.length);
    check(destIndex,LONG_NUM_BYTES, dest.length);

    long value = UNSAFE.getLong(src, BYTE_ARRAY_OFFSET + srcIndex);
    UNSAFE.putLong(dest, (BYTE_ARRAY_OFFSET + destIndex), value);
  }

  /**
   * Copy a short value to the dest+destIndex
   *
   * @param dest destination byte array
   * @param destIndex destination index
   * @param value a short value
   */
  public static void putShort(byte[] dest, int destIndex, short value) {
    check(destIndex, SHORT_NUM_BYTES, dest.length);
    UNSAFE.putShort(dest, (BYTE_ARRAY_OFFSET + destIndex), value);
  }

  /**
   * Copy an integer value to the dest+destIndex
   *
   * @param dest destination byte array
   * @param destIndex destination index
   * @param value an int value
   */
  public static void putInt(byte[] dest, int destIndex, int value) {
    check(destIndex, INT_NUM_BYTES, dest.length);
    UNSAFE.putInt(dest, (BYTE_ARRAY_OFFSET + destIndex), value);
  }

  /**
   * Copy a long value to the dest+destIndex
   *
   * @param dest destination byte array
   * @param destIndex destination index
   * @param value a long value
   */
  public static void putLong(byte[] dest, int destIndex, long value) {
    check(destIndex, LONG_NUM_BYTES, dest.length);
    UNSAFE.putLong(dest, (BYTE_ARRAY_OFFSET + destIndex), value);
  }

  // --------------------------------------------------------------------------
  // The following methods unsafe methods are not checked so that we
  // can compare their raw speed vs the checked ones
  // --------------------------------------------------------------------------

  public static byte getByte(long address) {
    return UNSAFE.getByte(address);
  }

  public static void putByte(long address, byte value) {
    UNSAFE.putByte(address, value);
  }

  public static int getInt(long address) {
    return UNSAFE.getInt(address);
  }

  public static void putInt(long address, int value) {
    UNSAFE.putInt(address, value);
  }

  public static long getLong(long address) {
    return UNSAFE.getLong(address);
  }

  public static void putLong(long address, long value) {
    UNSAFE.putLong(address, value);
  }

  public static void copyMemory(long srcAddr, long dstAddr, long length) {
    UNSAFE.copyMemory(srcAddr, dstAddr, length);
  }

  public static void copyMemory(long srcAddr, byte[] dst, int dstIndex, long length) {
    UNSAFE.copyMemory(null, srcAddr, dst, BYTE_ARRAY_OFFSET + dstIndex, length);
  }

  public static void copyMemory(byte[] src, int srcIndex, long dstAddr, long length) {
    UNSAFE.copyMemory(src, BYTE_ARRAY_OFFSET + srcIndex, null, dstAddr, length);
  }

  public static long allocateMemory(long size) {
    return UNSAFE.allocateMemory(size);
  }

  public static void freeMemory(long address) {
    UNSAFE.freeMemory(address);
  }

// ----------------------------------------------------------------------------
// Local Implementation
// ----------------------------------------------------------------------------

  private static void check(int index, int len, int bufferLen) {
    if (BoundsChecking.BOUNDS_CHECKING_ENABLED) {
      if (index < 0 || len < 0) {
        throw new IllegalArgumentException(String.format("index: [%d], len: [%d]", index, len));
      }
      if ((index + len) > bufferLen) {
        throw new IllegalArgumentException(String.format("Trying to access more than buffer length; index: [%d], len: [%d], buffer-len: [%d]", index, len, bufferLen));
      }
    }
  }

  /** Disable class instantiation */
  private MemoryUtils() {
  }

}
