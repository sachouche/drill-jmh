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

import org.apache.drill.jmh.memory.BulkMemoryAccessBaseTests;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.infra.Blackhole;

import org.apache.drill.jmh.common.state.ByteMemoryAccessorState;
import org.apache.drill.jmh.common.utils.FastAccessUtils;
import org.apache.drill.jmh.common.utils.JMHUtils;
import org.apache.drill.jmh.common.utils.JMHUtils.JMHLaunchContext;
import org.apache.drill.jmh.common.utils.JMHUtils.JMHLaunchContextBuilder;
import org.apache.drill.jmh.common.utils.MemoryUtils;

// Test Characteristics -
// - Input         : fixed & variable length data (configurable)
// - Processing    : bulk getter and setter APIs
// - Data Structure:
//   a) Direct memory using UNSAFE, Netty, and DrillBuf
//   b) Hybrid (UNSAFE, Netty, and DrillBuf)
//   c) Java heap arrays
public class BulkMemoryAccessWithIntermediaryBuffsBenchmark extends BulkMemoryAccessBaseTests {

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void getBytesDirectDataUsingIntermediaryBuffers(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    // Copy a chunk of data from source direct memory to source intermediary heap memory
    MemoryUtils.copyMemory(state.srcDirectBuffer + state.position, state.srcIntermediaryBuffer, 0, StateData.CHUNK_SZ);

    final int copyLen       = state.getCopyLen();
    final int upperBoundary = StateData.CHUNK_SZ - copyLen;

    for (int idx = 0; idx < upperBoundary; idx += copyLen) {
      if (copyLen <= 8) {
        FastAccessUtils.vlCopyLELong(state.srcIntermediaryBuffer, idx, state.tgtIntermediaryBuffer, idx, copyLen);
      } else {
        FastAccessUtils.vlCopyGTLong(state.srcIntermediaryBuffer, idx, state.tgtIntermediaryBuffer, idx, copyLen);
      }
      blackhole.consume(state.tgtIntermediaryBuffer);
    }

    // Copy a chunk of data from target intermediary heap memory to target direct memory
    MemoryUtils.copyMemory(state.tgtIntermediaryBuffer, 0, state.tgtDirectBuffer + state.position, StateData.CHUNK_SZ);
    blackhole.consume(state.tgtDirectBuffer);

    // Update the position
    state.position += StateData.CHUNK_SZ;
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void getBytesDirectDataUsingIntermediaryBuffersNetty(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    // Copy a chunk of data from source direct memory to source intermediary heap memory
    state.srcDirectBufferNetty.getBytes(state.position,  state.srcIntermediaryBuffer, 0, StateData.CHUNK_SZ);

    final int copyLen       = state.getCopyLen();
    final int upperBoundary = StateData.CHUNK_SZ  - copyLen;

    for (int idx = 0; idx < upperBoundary; idx += copyLen) {
      if (copyLen <= 8) {
        FastAccessUtils.vlCopyLELong(state.srcIntermediaryBuffer, idx, state.tgtIntermediaryBuffer, idx, copyLen);
      } else {
        FastAccessUtils.vlCopyGTLong(state.srcIntermediaryBuffer, idx, state.tgtIntermediaryBuffer, idx, copyLen);
      }
      blackhole.consume(state.tgtIntermediaryBuffer);
    }

    // Copy a chunk of data from target intermediary heap memory to target direct memory
    state.tgtDirectBufferNetty.setBytes(state.position, state.tgtIntermediaryBuffer, 0, StateData.CHUNK_SZ);
    blackhole.consume(state.tgtDirectBufferNetty);

    // Update the position
    state.position += StateData.CHUNK_SZ;
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void getBytesDirectDataUsingIntermediaryBuffersDrill(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    // Copy a chunk of data from source direct memory to source intermediary heap memory
    state.srcDirectBufferDrill.getBytes(state.position,  state.srcIntermediaryBuffer, 0, StateData.CHUNK_SZ);

    final int copyLen       = state.getCopyLen();
    final int upperBoundary = StateData.CHUNK_SZ  - copyLen;

    for (int idx = 0; idx < upperBoundary; idx += copyLen) {
      if (copyLen <= 8) {
        FastAccessUtils.vlCopyLELong(state.srcIntermediaryBuffer, idx, state.tgtIntermediaryBuffer, idx, copyLen);
      } else {
        FastAccessUtils.vlCopyGTLong(state.srcIntermediaryBuffer, idx, state.tgtIntermediaryBuffer, idx, copyLen);
      }
      blackhole.consume(state.tgtIntermediaryBuffer);
    }

    // Copy a chunk of data from target intermediary heap memory to target direct memory
    state.tgtDirectBufferDrill.setBytes(state.position, state.tgtIntermediaryBuffer, 0, StateData.CHUNK_SZ);
    blackhole.consume(state.tgtDirectBufferDrill);

    // Update the position
    state.position += StateData.CHUNK_SZ;
  }


  public static void main(String... args) throws Exception {
    JMHLaunchContextBuilder builder = new JMHLaunchContextBuilder(args);
    builder.addMemoryBulkOptions(); // add custom options
    builder.addTestFilter(BulkMemoryAccessWithIntermediaryBuffsBenchmark.class.getName());
    JMHLaunchContext launchContext  = builder.build();

    if (launchContext.testMode) {
      test();
    } else if (launchContext.opts != null) {
      new org.openjdk.jmh.runner.Runner(launchContext.opts).run();
    }
  }

  private static void test() {
    StateData stateData                  = new StateData();
    BulkMemoryAccessWithIntermediaryBuffsBenchmark benchmark = new BulkMemoryAccessWithIntermediaryBuffsBenchmark();
    Blackhole blackHole                  = JMHUtils.newBlackhole();

    stateData.doSetup();

    // run the Direct Memory benchmark
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      benchmark.getBytesDirectData(stateData, blackHole);
    }
    ByteMemoryAccessorState.printDMData(stateData);

    // run the Direct Memory benchmark Netty
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      benchmark.getBytesDirectDataNetty(stateData, blackHole);
    }
    ByteMemoryAccessorState.printDMDataNetty(stateData);

    // run the Direct Memory benchmark Drill
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      benchmark.getBytesDirectDataDrill(stateData, blackHole);
    }
    ByteMemoryAccessorState.printDMDataDrill(stateData);

    // run the Direct Memory with Intermediary Buffers benchmark
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      benchmark.getBytesDirectDataUsingIntermediaryBuffers(stateData, blackHole);
    }
    ByteMemoryAccessorState.printDMWithHBData(stateData);

    // run the Direct Memory Netty with Intermediary Buffers benchmark
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      benchmark.getBytesDirectDataUsingIntermediaryBuffersNetty(stateData, blackHole);
    }
    ByteMemoryAccessorState.printDMWithHBDataNetty(stateData);

    // run the Direct Memory Drill with Intermediary Buffers benchmark
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      benchmark.getBytesDirectDataUsingIntermediaryBuffersDrill(stateData, blackHole);
    }
    ByteMemoryAccessorState.printDMWithHBDataDrill(stateData);

  }

}
