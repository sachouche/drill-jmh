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
package org.apache.drill.jmh.advanced.expr;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

import org.apache.drill.jmh.common.state.ByteMemoryAccessorState;
import org.apache.drill.jmh.common.utils.JMHUtils.JMHLaunchContext;
import org.apache.drill.jmh.common.utils.JMHUtils.JMHLaunchContextBuilder;
import org.apache.drill.jmh.common.utils.MemoryUtils;

import io.netty.buffer.ByteBuf;

// Test Characteristics -
// - Input         : fixed length
// - Processing    : implement "contains-substring"
// - Data Structure:
//   a) Direct memory using UNSAFE, Netty, and DrillBuf
//   b) Hybrid (UNSAFE, Netty, and DrillBuf)
//   c) Java heap arrays

public class ExprMemoryAccessBenchmark {

  private static final int         ENTRY_LEN = 4;
  private static final byte firstPatternByte = 'G';

  @State(Scope.Thread)
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public static class StateData extends ByteMemoryAccessorState {
    private final byte[] _seedData = {'a', 'b', 'c', 'd', '1', '2', '3', '4'};

    @Setup(Level.Trial)
    public void doSetup() {
      this.seedData = _seedData;
      super.doSetup();
    }

    @TearDown(Level.Trial)
    public void doTearDown() {
      super.doTearDown();
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public int matchDirectData(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    final int upperBoundary = state.position + StateData.CHUNK_SZ;
    int numMatches          = 0;
    for (; state.position < upperBoundary; state.position += ENTRY_LEN) {
      int matches = match(state.position, state.position + ENTRY_LEN, state.srcDirectBuffer);

      if (matches > 0) {
        ++numMatches;
      }
    }

    blackhole.consume(numMatches);
    return numMatches;
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public int matchDirectDataNetty(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    final int upperBoundary = state.position + StateData.CHUNK_SZ;
    int numMatches          = 0;
    for (; state.position < upperBoundary; state.position += ENTRY_LEN) {
      int matches = match(state.position, state.position + ENTRY_LEN, state.srcDirectBufferNetty);

      if (matches > 0) {
        ++numMatches;
      }
    }

    blackhole.consume(numMatches);
    return numMatches;
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public int matchDirectDataDrill(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }
    final int upperBoundary = state.position + StateData.CHUNK_SZ;
    int numMatches          = 0;
    for (; state.position < upperBoundary; state.position += ENTRY_LEN) {
      int matches = match(state.position, state.position + ENTRY_LEN, state.srcDirectBufferDrill);

      if (matches > 0) {
        ++numMatches;
      }
    }

    blackhole.consume(numMatches);
    return numMatches;
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public int matchDirectDataUsingIntermediaryBuffers(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    // Copy a chunk of data from source direct memory to source intermediary heap memory
    MemoryUtils.copyMemory(state.srcDirectBuffer + state.position, state.srcIntermediaryBuffer, 0, StateData.CHUNK_SZ);

    int numMatches = 0;
    for (int idx = 0; idx < StateData.CHUNK_SZ; idx += ENTRY_LEN) {
      int matches = match(idx, idx + ENTRY_LEN, state.srcIntermediaryBuffer);

      if (matches > 0) {
        ++numMatches;
      }
    }

    // Update the position
    state.position += StateData.CHUNK_SZ;

    blackhole.consume(numMatches);
    return numMatches;
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public int matchDirectDataUsingIntermediaryBuffersNetty(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    // Copy a chunk of data from source direct memory to source intermediary heap memory
    state.srcDirectBufferNetty.getBytes(state.position,  state.srcIntermediaryBuffer, 0, StateData.CHUNK_SZ);

    int numMatches = 0;
    for (int idx = 0; idx < StateData.CHUNK_SZ; idx += ENTRY_LEN) {
      int matches = match(idx, idx + ENTRY_LEN, state.srcIntermediaryBuffer);

      if (matches > 0) {
        ++numMatches;
      }
    }

    // Update the position
    state.position += StateData.CHUNK_SZ;

    blackhole.consume(numMatches);
    return numMatches;

  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public int matchDirectDataUsingIntermediaryBuffersDrill(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    // Copy a chunk of data from source direct memory to source intermediary heap memory
    state.srcDirectBufferDrill.getBytes(state.position,  state.srcIntermediaryBuffer, 0, StateData.CHUNK_SZ);

    int numMatches = 0;
    for (int idx = 0; idx < StateData.CHUNK_SZ; idx += ENTRY_LEN) {
      int matches = match(idx, idx + ENTRY_LEN, state.srcIntermediaryBuffer);

      if (matches > 0) {
        ++numMatches;
      }
    }

    // Update the position
    state.position += StateData.CHUNK_SZ;

    blackhole.consume(numMatches);
    return numMatches;
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public int matchHeapData(StateData state, Blackhole blackhole) {
    if ((state.srcHeapByteArray.length - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    final int upperBoundary = state.position + StateData.CHUNK_SZ;
    int numMatches          = 0;
    for (; state.position < upperBoundary; state.position += ENTRY_LEN) {
      int matches = match(state.position, state.position + ENTRY_LEN, state.srcHeapByteArray);

      if (matches > 0) {
        ++numMatches;
      }
    }

    blackhole.consume(numMatches);
    return numMatches;
  }

  private static final int match(int start, int end, long buffer) {
    final int lengthToProcess   = end - start;

    // simplePattern string has meta characters i.e % and _ and escape characters removed.
    // so, we can just directly compare.
    for (int idx = 0; idx < lengthToProcess; idx++) {
      byte inputByte = MemoryUtils.getByte(buffer + start + idx);

      if (firstPatternByte != inputByte) {
        continue;
      }
      return 1;
    }
    return 0;
  }

  private static final int match(int start, int end, ByteBuf buffer) {
    final int lengthToProcess = end - start;

    // simplePattern string has meta characters i.e % and _ and escape characters removed.
    // so, we can just directly compare.
    for (int idx = 0; idx < lengthToProcess; idx++) {
      byte inputByte = buffer.getByte(start + idx);

      if (firstPatternByte != inputByte) {
        continue;
      }
      return 1;
    }
    return 0;
  }

  private static final int match(int start, int end, byte[] buffer) {
    final int lengthToProcess = end - start;

    // simplePattern string has meta characters i.e % and _ and escape characters removed.
    // so, we can just directly compare.
    for (int idx = 0; idx < lengthToProcess; idx++) {
      byte inputByte = buffer[start + idx];

      if (firstPatternByte != inputByte) {
        continue;
      }
      return 1;
    }
    return 0;
  }

  public static void main(String... args) throws Exception {
    JMHLaunchContextBuilder builder = new JMHLaunchContextBuilder(args);
    JMHLaunchContext launchContext  = builder.build();

    if (launchContext.testMode) {
      test();
    } else if (launchContext.opts != null) {
      new org.openjdk.jmh.runner.Runner(launchContext.opts).run();
    }
  }

  private static void test() {
    StateData stateData    = new StateData();
    ExprMemoryAccessBenchmark benchmark = new ExprMemoryAccessBenchmark();
    Blackhole blackHole    = new Blackhole("Today's password is swordfish. I understand instantiating Blackholes directly is dangerous.");

    stateData.doSetup();

    // run the Direct Memory benchmark
    int numMatches  = 0;
    String testType = "Direct Memory Test";
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      numMatches += benchmark.matchDirectData(stateData, blackHole);
    }
    ByteMemoryAccessorState.log(String.format("%s: Number of matches [%d]", testType, numMatches));

    // run the Direct Memory benchmark Netty
    numMatches = 0;
    testType   = "Direct Memory Test";
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      numMatches += benchmark.matchDirectDataNetty(stateData, blackHole);
    }
    ByteMemoryAccessorState.log(String.format("%s: Number of matches [%d]", testType, numMatches));

    // run the Direct Memory benchmark Drill
    numMatches = 0;
    testType   = "Direct Memory Test";
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      numMatches += benchmark.matchDirectDataDrill(stateData, blackHole);
    }
    ByteMemoryAccessorState.log(String.format("%s: Number of matches [%d]", testType, numMatches));

    // run the Direct Memory with Intermediary Buffers benchmark
    numMatches = 0;
    testType   = "Direct Memory Test";
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      numMatches += benchmark.matchDirectDataUsingIntermediaryBuffers(stateData, blackHole);
    }
    ByteMemoryAccessorState.log(String.format("%s: Number of matches [%d]", testType, numMatches));

    // run the Direct Memory Netty with Intermediary Buffers benchmark
    numMatches = 0;
    testType   = "Direct Memory Test";
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      numMatches += benchmark.matchDirectDataUsingIntermediaryBuffersNetty(stateData, blackHole);
    }
    ByteMemoryAccessorState.log(String.format("%s: Number of matches [%d]", testType, numMatches));

    // run the Direct Memory Drill with Intermediary Buffers benchmark
    numMatches = 0;
    testType   = "Direct Memory Test";
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      numMatches += benchmark.matchDirectDataUsingIntermediaryBuffersDrill(stateData, blackHole);
    }
    ByteMemoryAccessorState.log(String.format("%s: Number of matches [%d]", testType, numMatches));

    // run the Heap Memory benchmark
    numMatches = 0;
    testType   = "Direct Memory Test";
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      numMatches += benchmark.matchHeapData(stateData,   blackHole);
    }
    ByteMemoryAccessorState.log(String.format("%s: Number of matches [%d]", testType, numMatches));
  }

}
