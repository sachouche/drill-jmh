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
package org.apache.drill.jmh.memory;

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
import org.apache.drill.jmh.common.utils.MemoryUtils;

/**
 * Base class for the bulk memory access tests (to allow test reuse)
 */
public abstract class BulkMemoryAccessBaseTests {

  @State(Scope.Thread)
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public static class StateData extends ByteMemoryAccessorState {
    private final byte[] _seedData = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p'};

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

  // --------------------------------------------------------------------------
  // Copy bytes tests
  // --------------------------------------------------------------------------

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void getBytesDirectData(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    final int copyLen       = state.getCopyLen();
    final int upperBoundary = state.position + StateData.CHUNK_SZ - copyLen;

    for (int idx = state.position; idx < upperBoundary; idx += copyLen) {
      MemoryUtils.copyMemory(state.srcDirectBuffer + idx, state.tgtDirectBuffer + idx, copyLen);
      blackhole.consume(state.tgtDirectBuffer);
    }
    state.position = upperBoundary;

    blackhole.consume(state.tgtDirectBuffer);
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void getBytesDirectDataNetty(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    final int copyLen       = state.getCopyLen();
    final int upperBoundary = state.position + StateData.CHUNK_SZ - copyLen;

    for (int idx = state.position; idx < upperBoundary; idx += copyLen) {
      state.srcDirectBufferNetty.getBytes(idx, state.tgtDirectBufferNetty, idx, copyLen);
      blackhole.consume(state.tgtDirectBufferNetty);
    }
    state.position = upperBoundary;

    blackhole.consume(state.tgtDirectBufferNetty);
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void getBytesDirectDataDrill(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    final int copyLen       = state.getCopyLen();
    final int upperBoundary = state.position + StateData.CHUNK_SZ  - copyLen;

    for (int idx = state.position; idx < upperBoundary; idx += copyLen) {
      state.srcDirectBufferDrill.getBytes(idx, state.tgtDirectBufferDrill, idx, copyLen);
      blackhole.consume(state.tgtDirectBufferDrill);
    }
    state.position = upperBoundary;

    blackhole.consume(state.tgtDirectBufferDrill);
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void getBytesHeapData(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    final int copyLen       = state.getCopyLen();
    final int upperBoundary = state.position + StateData.CHUNK_SZ - copyLen;

    for (int idx = state.position; idx < upperBoundary; idx += copyLen) {
      System.arraycopy(state.srcHeapByteArray, idx, state.tgtHeapByteArray, idx, copyLen);
      blackhole.consume(state.tgtHeapByteArray);
    }
    state.position = upperBoundary;

    blackhole.consume(state.tgtHeapByteArray);
  }


}