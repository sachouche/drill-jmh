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
package org.apache.drill.jmh.common.state;


import java.util.Random;

import org.apache.drill.exec.memory.RootAllocator;
import org.apache.drill.jmh.common.utils.JMHUtils;
import org.apache.drill.jmh.common.utils.MemoryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.DrillBuf;
import io.netty.buffer.Unpooled;

public class ByteMemoryAccessorState {
  private static final Logger logger = LoggerFactory.getLogger(ByteMemoryAccessorState.class.getCanonicalName());

  public final static int CHUNK_SZ = 1 << 12;
  public final static int DATA_SZ  = 1 << 27;
  public byte[] seedData;

  public int position; // position within buffer

  public long srcDirectBuffer;
  public long tgtDirectBuffer;

  public ByteBuf srcDirectBufferNetty;
  public ByteBuf tgtDirectBufferNetty;

  public RootAllocator drillRootAllocator;
  public DrillBuf srcDirectBufferDrill;
  public DrillBuf tgtDirectBufferDrill;

  public byte[] srcHeapByteArray;
  public byte[] tgtHeapByteArray;

  public int[] srcHeapIntArray;
  public int[] tgtHeapIntArray;

  public long[] srcHeapLongArray;
  public long[] tgtHeapLongArray;

  public byte[] srcIntermediaryBuffer;
  public byte[] tgtIntermediaryBuffer;

  // The size of the bulk access
  public int bulkAccessSize           = JMHUtils.DEFAULT_MEMORY_ACCESS_LEN;
  public boolean enableVariableLength = false;
  public int[] randNumbers            = null;
  private int currRandIdx             = 0;

  public void doSetup() {
    if (seedData == null
     || seedData.length  < 8
     || (seedData.length % 8) > 0) {

      throw new RuntimeException("You need to set the seed data; it should be a multiple of 8..");
    }
    position = 0;
    initStateFromEnv();
    initDataStructure();
  }

  public void doTearDown() {
    if (srcDirectBuffer != 0) {
      MemoryUtils.freeMemory(srcDirectBuffer);
      srcDirectBuffer = 0L;
    }
    if (tgtDirectBuffer != 0) {
      MemoryUtils.freeMemory(tgtDirectBuffer);
      tgtDirectBuffer = 0L;
    }
    if (srcDirectBufferNetty != null) {
      srcDirectBufferNetty.release();
      srcDirectBufferNetty = null;
    }
    if (tgtDirectBufferNetty != null) {
      tgtDirectBufferNetty.release();
      tgtDirectBufferNetty = null;
    }
    if (srcDirectBufferDrill != null) {
      srcDirectBufferDrill.release();
      srcDirectBufferDrill = null;
    }
    if (tgtDirectBufferDrill != null) {
      tgtDirectBufferDrill.release();
      tgtDirectBufferDrill = null;
    }
    if (drillRootAllocator != null) {
      drillRootAllocator.close();
    }
  }

  public static Logger getLogger() {
    return logger;
  }

  public void initDataStructure() {
    // Allocate direct memory

    // Directly from Java
    srcDirectBuffer  = MemoryUtils.allocateMemory(DATA_SZ);
    tgtDirectBuffer  = MemoryUtils.allocateMemory(DATA_SZ);

    // From Netty
    srcDirectBufferNetty = Unpooled.directBuffer(DATA_SZ);
    tgtDirectBufferNetty = Unpooled.directBuffer(DATA_SZ);

    // From Drill
    drillRootAllocator   = new RootAllocator(DATA_SZ * 4);
    srcDirectBufferDrill = drillRootAllocator.buffer(DATA_SZ);
    tgtDirectBufferDrill = drillRootAllocator.buffer(DATA_SZ);

    // Intermediary heap buffers
    final int PADDING     = 0x8;
    srcIntermediaryBuffer = new byte[CHUNK_SZ + PADDING];
    tgtIntermediaryBuffer = new byte[CHUNK_SZ + PADDING];

    // Heap arrays
    srcHeapByteArray = new byte[DATA_SZ];
    tgtHeapByteArray = new byte[DATA_SZ];
    srcHeapIntArray  = new int [DATA_SZ / 4];
    tgtHeapIntArray  = new int [DATA_SZ / 4];
    srcHeapLongArray = new long[DATA_SZ / 8];
    tgtHeapLongArray = new long[DATA_SZ / 8];

    // Each seed entry will be recorded as a long within the byte array;
    // this is mainly done to ensure that heap & direct memory buffers
    // can replay the same data (which simplifies validation).
    for (int idx1 = 0; idx1 < DATA_SZ; idx1 += (seedData.length * 8)) {
      for (int idx2 = 0; idx2 < seedData.length; idx2++) {

        // Direct Memory buffers
        MemoryUtils.putLong(srcDirectBuffer + idx1 + idx2 * 8, seedData[idx2]);
        srcDirectBufferNetty.setLong(idx1 + idx2 * 8, seedData[idx2]);
        srcDirectBufferDrill.setLong(idx1 + idx2 * 8, seedData[idx2]);

        // Heap arrays
        MemoryUtils.putLong(srcHeapByteArray, idx1 + idx2 * 8, seedData[idx2]);
        srcHeapIntArray[(idx1 / 4) + 2 * idx2]      = seedData[idx2];
        srcHeapIntArray[(idx1 / 4) + 2 * idx2 + 1]  = 0;
        srcHeapLongArray[(idx1 / 8) + idx2]         = seedData[idx2];
      }
    }
  }

  private void initStateFromEnv() {
    final String entryLenStr  = System.getProperty(JMHUtils.MEMORY_ACCESS_LEN_JVM_OPTION);
    final String enableVarLen = System.getProperty(JMHUtils.MEMORY_ACCESS_VAR_LEN_JVM_OPTION);

    if (entryLenStr != null) {
      try {
        bulkAccessSize = Integer.parseInt(entryLenStr);

        if (bulkAccessSize > CHUNK_SZ) {
          logger.warn(String.format("The bulk memory length cannot exceed [%d]; using [%d] instead..",
            CHUNK_SZ, CHUNK_SZ));

          bulkAccessSize = CHUNK_SZ;
        }
      } catch (Exception e) {
        // NOOP
      }
    }

    if (enableVarLen != null && "true".equalsIgnoreCase(enableVarLen)) {
      enableVariableLength = true;

      // Initialize an array of random numbers
      randNumbers = new int[CHUNK_SZ];
      Random rand = new Random();

      for (int idx = 0; idx < randNumbers.length; idx++) {
        randNumbers[idx] = rand.nextInt(bulkAccessSize) + 1;
      }
    }
  }

  public int getCopyLen() {
    if (enableVariableLength) {
      return randNumbers[currRandIdx++ % randNumbers.length];
    }
    return bulkAccessSize;
  }


  public static void printDMData(ByteMemoryAccessorState stateData) {
    final int startIdx = 1 << 20;

    logger.info("Dumping direct memory test data..");
    for (int idx = startIdx; idx < (startIdx + 16); idx++) {
      logger.info(String.format("\ttarget[%d] --> %c", idx, MemoryUtils.getByte(stateData.tgtDirectBuffer + idx)));
    }
  }

  public static void printDMDataNetty(ByteMemoryAccessorState stateData) {
    final int startIdx = 1 << 20;

    logger.info("Dumping direct memory (Netty) test data..");
    for (int idx = startIdx; idx < (startIdx + 16); idx++) {
      logger.info(String.format("\ttarget[%d] --> %c", idx, stateData.tgtDirectBufferNetty.getByte(idx)));
    }
  }

  public static void printDMDataDrill(ByteMemoryAccessorState stateData) {
    final int startIdx = 1 << 20;

    logger.info("Dumping direct memory (Drill) test data..");
    for (int idx = startIdx; idx < (startIdx + 16); idx++) {
      logger.info(String.format("\ttarget[%d] --> %c", idx, stateData.tgtDirectBufferDrill.getByte(idx)));
    }
  }

  public static void printDMWithHBData(ByteMemoryAccessorState stateData) {
    final int startIdx = 1 << 20;

    logger.info("Dumping direct memory with intermediary buffers test data..");
    for (int idx = startIdx; idx < (startIdx + 16); idx++) {
      logger.info(String.format("\ttarget[%d] --> %c", idx, MemoryUtils.getByte(stateData.tgtDirectBuffer + idx)));
    }
  }

  public static void printDMWithHBDataNetty(ByteMemoryAccessorState stateData) {
    final int startIdx = 1 << 20;

    logger.info("Dumping direct memory (Netty) with intermediary buffers test data..");
    for (int idx = startIdx; idx < (startIdx + 16); idx++) {
      logger.info(String.format("\ttarget[%d] --> %c", idx, stateData.tgtDirectBufferNetty.getByte(idx)));
    }
  }

  public static void printDMWithHBDataDrill(ByteMemoryAccessorState stateData) {
    final int startIdx = 1 << 20;

    logger.info("Dumping direct memory (Drill) with intermediary buffers test data..");
    for (int idx = startIdx; idx < (startIdx + 16); idx++) {
      logger.info(String.format("\ttarget[%d] --> %c", idx, stateData.tgtDirectBufferDrill.getByte(idx)));
    }
  }

  public static void printHeapData(ByteMemoryAccessorState stateData) {
    final int startIdx = 1 << 20;
    logger.info("Dumping heap memory test data..");
    for (int idx = startIdx; idx < (startIdx + 16); idx++) {
      logger.info(String.format("\ttarget[%d] --> %c", idx, stateData.tgtHeapByteArray[idx]));
    }
  }

  public static void log(final String msg) {
    logger.info(msg);
  }

}

