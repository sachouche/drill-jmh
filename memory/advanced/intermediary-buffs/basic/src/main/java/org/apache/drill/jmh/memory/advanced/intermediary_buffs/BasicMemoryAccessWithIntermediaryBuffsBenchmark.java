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
package org.apache.drill.jmh.memory.advanced.intermediary_buffs;

import org.apache.drill.jmh.memory.BasicMemoryAccessBaseTests;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.infra.Blackhole;

import org.apache.drill.jmh.common.state.ByteMemoryAccessorState;
import org.apache.drill.jmh.common.utils.JMHUtils;
import org.apache.drill.jmh.common.utils.JMHUtils.JMHLaunchContext;
import org.apache.drill.jmh.common.utils.JMHUtils.JMHLaunchContextBuilder;
import org.apache.drill.jmh.common.utils.MemoryUtils;

// Test Characteristics -
// - Input         : fixed length
// - Processing    : invoke the basic memory access APIs & equivalent functionality using intermediary buffers
// - Data Structure:
//   a) Direct memory using UNSAFE, Netty, and DrillBuf
//   b) Hybrid (UNSAFE, Netty, and DrillBuf)
//   c) Java heap arrays

public class BasicMemoryAccessWithIntermediaryBuffsBenchmark extends BasicMemoryAccessBaseTests {

  // --------------------------------------------------------------------------
  // Single byte read tests
  // --------------------------------------------------------------------------

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void getByteDirectDataUsingIntermediaryBuffers(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    // Copy a chunk of data from source direct memory to source intermediary heap memory
    MemoryUtils.copyMemory(state.srcDirectBuffer + state.position, state.srcIntermediaryBuffer, 0, StateData.CHUNK_SZ);

    for (int idx = 0; idx < StateData.CHUNK_SZ; idx++) {
      final byte value = state.srcIntermediaryBuffer[idx];
      blackhole.consume(value);
    }

    // Update the position
    state.position += StateData.CHUNK_SZ;
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void getByteDirectDataUsingIntermediaryBuffersNetty(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    // Copy a chunk of data from source direct memory to source intermediary heap memory
    state.srcDirectBufferNetty.getBytes(state.position,  state.srcIntermediaryBuffer, 0, StateData.CHUNK_SZ);

    for (int idx = 0; idx < StateData.CHUNK_SZ; idx++) {
      final byte value = state.srcIntermediaryBuffer[idx];
      blackhole.consume(value);
    }

    // Update the position
    state.position += StateData.CHUNK_SZ;
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void getByteDirectDataUsingIntermediaryBuffersDrill(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    // Copy a chunk of data from source direct memory to source intermediary heap memory
    state.srcDirectBufferDrill.getBytes(state.position,  state.srcIntermediaryBuffer, 0, StateData.CHUNK_SZ);

    for (int idx = 0; idx < StateData.CHUNK_SZ; idx++) {
      final byte value = state.srcIntermediaryBuffer[idx];
      blackhole.consume(value);
    }

    // Update the position
    state.position += StateData.CHUNK_SZ;
  }

  // --------------------------------------------------------------------------
  // Single byte write tests
  // --------------------------------------------------------------------------

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void setByteDirectDataUsingIntermediaryBuffers(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    final byte value = 'a';
    for (int idx = 0; idx < StateData.CHUNK_SZ; idx++) {
      state.tgtIntermediaryBuffer[idx] = value;
      blackhole.consume(state.tgtIntermediaryBuffer);
    }

    // Copy a chunk of data from intermediary heap memory to target direct memory
    MemoryUtils.copyMemory(state.tgtIntermediaryBuffer, 0, state.tgtDirectBuffer + state.position, StateData.CHUNK_SZ);

    // Update the position
    state.position += StateData.CHUNK_SZ;
    blackhole.consume(state.tgtDirectBuffer);
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void setByteDirectDataUsingIntermediaryBuffersNetty(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    final byte value = 'a';
    for (int idx = 0; idx < StateData.CHUNK_SZ; idx++) {
      state.tgtIntermediaryBuffer[idx] = value;
      blackhole.consume(state.tgtIntermediaryBuffer);
    }

    // Copy a chunk of data from intermediary heap memory to target direct memory
    state.tgtDirectBufferNetty.setBytes(state.position, state.tgtIntermediaryBuffer, 0, StateData.CHUNK_SZ);

    // Update the position
    state.position += StateData.CHUNK_SZ;
    blackhole.consume(state.tgtDirectBufferNetty);
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void setByteDirectDataUsingIntermediaryBuffersDrill(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    final byte value = 'a';
    for (int idx = 0; idx < StateData.CHUNK_SZ; idx++) {
      state.tgtIntermediaryBuffer[idx] = value;
      blackhole.consume(state.tgtIntermediaryBuffer);
    }

    // Copy a chunk of data from intermediary heap memory to target direct memory
    state.tgtDirectBufferDrill.setBytes(state.position, state.tgtIntermediaryBuffer, 0, StateData.CHUNK_SZ);

    // Update the position
    state.position += StateData.CHUNK_SZ;
    blackhole.consume(state.tgtDirectBufferDrill);
  }

  // --------------------------------------------------------------------------
  // Single integer read tests
  // --------------------------------------------------------------------------

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void getIntDirectDataUsingIntermediaryBuffers(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    // Copy a chunk of data from source direct memory to source intermediary heap memory
    MemoryUtils.copyMemory(state.srcDirectBuffer + state.position, state.srcIntermediaryBuffer, 0, StateData.CHUNK_SZ);

    for (int idx = 0; idx < StateData.CHUNK_SZ; idx += MemoryUtils.INT_NUM_BYTES) {
      final int value = MemoryUtils.getInt(state.srcIntermediaryBuffer, idx);
      blackhole.consume(value);
    }
    state.position += StateData.CHUNK_SZ;
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void getIntDirectDataUsingIntermediaryBuffersNetty(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    // Copy a chunk of data from source direct memory to source intermediary heap memory
    state.srcDirectBufferNetty.getBytes(state.position,  state.srcIntermediaryBuffer, 0, StateData.CHUNK_SZ);

    for (int idx = 0; idx < StateData.CHUNK_SZ; idx += MemoryUtils.INT_NUM_BYTES) {
      final int value = MemoryUtils.getInt(state.srcIntermediaryBuffer, idx);
      blackhole.consume(value);
    }
    state.position += StateData.CHUNK_SZ;
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void getIntDirectDataUsingIntermediaryBuffersDrill(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    // Copy a chunk of data from source direct memory to source intermediary heap memory
    state.srcDirectBufferDrill.getBytes(state.position,  state.srcIntermediaryBuffer, 0, StateData.CHUNK_SZ);

    for (int idx = 0; idx < StateData.CHUNK_SZ; idx += MemoryUtils.INT_NUM_BYTES) {
      final int value = MemoryUtils.getInt(state.srcIntermediaryBuffer, idx);
      blackhole.consume(value);
    }
    state.position += StateData.CHUNK_SZ;
  }

  // --------------------------------------------------------------------------
  // Single integer write tests
  // --------------------------------------------------------------------------

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void setIntDirectDataUsingIntermediaryBuffers(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    final int value  = state.seedData[0];

    for (int idx = 0; idx < StateData.CHUNK_SZ; idx += MemoryUtils.INT_NUM_BYTES) {
      MemoryUtils.putInt(state.tgtIntermediaryBuffer, idx, value);
      blackhole.consume(state.tgtIntermediaryBuffer);
    }

    // Copy a chunk of data from intermediary heap memory to target direct memory
    MemoryUtils.copyMemory(state.tgtIntermediaryBuffer, 0, state.tgtDirectBuffer + state.position, StateData.CHUNK_SZ);

    // Update the position
    state.position += StateData.CHUNK_SZ;
    blackhole.consume(state.tgtDirectBuffer);
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void setIntDirectDataUsingIntermediaryBuffersNetty(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    final int value  = state.seedData[0];

    for (int idx = 0; idx < StateData.CHUNK_SZ; idx += MemoryUtils.INT_NUM_BYTES) {
      MemoryUtils.putInt(state.tgtIntermediaryBuffer, idx, value);
      blackhole.consume(state.tgtIntermediaryBuffer);
    }

    // Copy a chunk of data from intermediary heap memory to target direct memory
    state.tgtDirectBufferNetty.setBytes(state.position, state.tgtIntermediaryBuffer, 0, StateData.CHUNK_SZ);

    // Update the position
    state.position += StateData.CHUNK_SZ;
    blackhole.consume(state.tgtDirectBufferNetty);
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void setIntDirectDataUsingIntermediaryBuffersDrill(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    final int value  = state.seedData[0];

    for (int idx = 0; idx < StateData.CHUNK_SZ; idx += MemoryUtils.INT_NUM_BYTES) {
      MemoryUtils.putInt(state.tgtIntermediaryBuffer, idx, value);
      blackhole.consume(state.tgtIntermediaryBuffer);
    }

    // Copy a chunk of data from intermediary heap memory to target direct memory
    state.tgtDirectBufferDrill.setBytes(state.position, state.tgtIntermediaryBuffer, 0, StateData.CHUNK_SZ);

    // Update the position
    state.position += StateData.CHUNK_SZ;
    blackhole.consume(state.tgtDirectBufferDrill);
  }

  // --------------------------------------------------------------------------
  // Single long read tests
  // --------------------------------------------------------------------------

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void getLongDirectDataUsingIntermediaryBuffers(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    // Copy a chunk of data from source direct memory to source intermediary heap memory
    MemoryUtils.copyMemory(state.srcDirectBuffer + state.position, state.srcIntermediaryBuffer, 0, StateData.CHUNK_SZ);

    for (int idx = 0; idx < StateData.CHUNK_SZ; idx += MemoryUtils.LONG_NUM_BYTES) {
      final long value = MemoryUtils.getLong(state.srcIntermediaryBuffer, idx);
      blackhole.consume(value);
    }
    state.position += StateData.CHUNK_SZ;
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void getLongDirectDataUsingIntermediaryBuffersNetty(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    // Copy a chunk of data from source direct memory to source intermediary heap memory
    state.srcDirectBufferNetty.getBytes(state.position,  state.srcIntermediaryBuffer, 0, StateData.CHUNK_SZ);

    for (int idx = 0; idx < StateData.CHUNK_SZ; idx += MemoryUtils.LONG_NUM_BYTES) {
      final long value = MemoryUtils.getLong(state.srcIntermediaryBuffer, idx);
      blackhole.consume(value);
    }
    state.position += StateData.CHUNK_SZ;
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void getLongDirectDataUsingIntermediaryBuffersDrill(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    // Copy a chunk of data from source direct memory to source intermediary heap memory
    state.srcDirectBufferDrill.getBytes(state.position,  state.srcIntermediaryBuffer, 0, StateData.CHUNK_SZ);

    for (int idx = 0; idx < StateData.CHUNK_SZ; idx += MemoryUtils.LONG_NUM_BYTES) {
      final long value = MemoryUtils.getLong(state.srcIntermediaryBuffer, idx);
      blackhole.consume(value);
    }
    state.position += StateData.CHUNK_SZ;
  }

  // --------------------------------------------------------------------------
  // Single long write tests
  // --------------------------------------------------------------------------

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void setLongDirectDataUsingIntermediaryBuffers(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    final long value = state.seedData[0];

    for (int idx = 0; idx < StateData.CHUNK_SZ; idx += MemoryUtils.LONG_NUM_BYTES) {
      MemoryUtils.putLong(state.tgtIntermediaryBuffer, idx, value);
      blackhole.consume(state.tgtIntermediaryBuffer);
    }

    // Copy a chunk of data from intermediary heap memory to target direct memory
    MemoryUtils.copyMemory(state.tgtIntermediaryBuffer, 0, state.tgtDirectBuffer + state.position, StateData.CHUNK_SZ);

    // Update the position
    state.position += StateData.CHUNK_SZ;
    blackhole.consume(state.tgtDirectBuffer);
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void setLongDirectDataUsingIntermediaryBuffersNetty(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    final long value = state.seedData[0];

    for (int idx = 0; idx < StateData.CHUNK_SZ; idx += MemoryUtils.LONG_NUM_BYTES) {
      MemoryUtils.putLong(state.tgtIntermediaryBuffer, idx, value);
      blackhole.consume(state.tgtIntermediaryBuffer);
    }

    // Copy a chunk of data from intermediary heap memory to target direct memory
    state.tgtDirectBufferNetty.setBytes(state.position, state.tgtIntermediaryBuffer, 0, StateData.CHUNK_SZ);

    // Update the position
    state.position += StateData.CHUNK_SZ;
    blackhole.consume(state.tgtDirectBufferNetty);
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void setLongDirectDataUsingIntermediaryBuffersDrill(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    final long value = state.seedData[0];

    for (int idx = 0; idx < StateData.CHUNK_SZ; idx += MemoryUtils.LONG_NUM_BYTES) {
      MemoryUtils.putLong(state.tgtIntermediaryBuffer, idx, value);
      blackhole.consume(state.tgtIntermediaryBuffer);
    }

    // Copy a chunk of data from intermediary heap memory to target direct memory
    state.tgtDirectBufferDrill.setBytes(state.position, state.tgtIntermediaryBuffer, 0, StateData.CHUNK_SZ);

    // Update the position
    state.position += StateData.CHUNK_SZ;
    blackhole.consume(state.tgtDirectBufferDrill);
  }

  public static void main(String... args) throws Exception {
    JMHLaunchContextBuilder builder = new JMHLaunchContextBuilder(args);
    builder.addTestFilter(BasicMemoryAccessWithIntermediaryBuffsBenchmark.class.getName());
    JMHLaunchContext launchContext  = builder.build();

    if (launchContext.testMode) {
      test();
    } else if (launchContext.opts != null) {
      new org.openjdk.jmh.runner.Runner(launchContext.opts).run();
    }
  }

  private static void test() {
    StateData stateData                  = new StateData();
    BasicMemoryAccessWithIntermediaryBuffsBenchmark benchmark = new BasicMemoryAccessWithIntermediaryBuffsBenchmark();
    Blackhole blackHole                  = JMHUtils.newBlackhole();
    final String loggingPrefix           = "*** ";

    stateData.doSetup();

    //-------------------------------------------------------------------------
    // READ SINGLE BYTE TESTS
    // ------------------------------------------------------------------------

    StateData.getLogger().info(String.format("%sRunning the Single Read Byte tests..", loggingPrefix));

    // run the Direct Memory with Intermediary Buffers
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      benchmark.getByteDirectDataUsingIntermediaryBuffers(stateData, blackHole);
    }

    // run the Direct Memory (Netty) with Intermediary Buffers
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      benchmark.getByteDirectDataUsingIntermediaryBuffersNetty(stateData, blackHole);
    }

    // run the Direct Memory Drill with Intermediary Buffers
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      benchmark.getByteDirectDataUsingIntermediaryBuffersDrill(stateData, blackHole);
    }

    // run the Heap Memory benchmark
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      benchmark.getByteHeapData(stateData, blackHole);
    }

    //-------------------------------------------------------------------------
    // WRITE SINGLE BYTE TESTS
    // ------------------------------------------------------------------------

    StateData.getLogger().info(String.format("%sRunning the Single Write Byte tests..", loggingPrefix));

    // Direct Memory benchmark with Intermediary Buffers
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      benchmark.setByteDirectDataUsingIntermediaryBuffers(stateData, blackHole);
    }
    ByteMemoryAccessorState.printDMData(stateData);

    // Direct Memory benchmark (Drill) with Intermediary Buffers
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      benchmark.setByteDirectDataUsingIntermediaryBuffersDrill(stateData, blackHole);
    }
    ByteMemoryAccessorState.printDMDataDrill(stateData);

    // Direct Memory benchmark (Netty) with Intermediary Buffers
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      benchmark.setByteDirectDataUsingIntermediaryBuffersNetty(stateData, blackHole);
    }
    ByteMemoryAccessorState.printDMDataNetty(stateData);

  }

}
