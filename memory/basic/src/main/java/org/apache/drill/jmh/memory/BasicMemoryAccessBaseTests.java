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
 * Base class for the basic memory access tests (to allow test reuse)
 */
public class BasicMemoryAccessBaseTests {

  @State(Scope.Thread)
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public static class StateData extends ByteMemoryAccessorState {
    public final byte[] _seedData = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p'};

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
  // Single byte read tests
  // --------------------------------------------------------------------------

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void getByteDirectData(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    final int upperBoundary = state.position + StateData.CHUNK_SZ;

    for (int idx = state.position; idx < upperBoundary; idx++) {
      final byte value = MemoryUtils.getByte(state.srcDirectBuffer + idx);
      blackhole.consume(value);
    }
    state.position = upperBoundary;
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void getByteDirectDataNetty(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    final int upperBoundary = state.position + StateData.CHUNK_SZ;
    for (int idx = state.position; idx < upperBoundary; idx++) {
      final byte value = state.srcDirectBufferNetty.getByte(idx);
      blackhole.consume(value);
    }
    state.position = upperBoundary;
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void getByteDirectDataDrill(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    final int upperBoundary = state.position + StateData.CHUNK_SZ;
    for (int idx = state.position; idx < upperBoundary; idx++) {
      final byte value = state.srcDirectBufferDrill.getByte(idx);
      blackhole.consume(value);
    }
    state.position = upperBoundary;
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void getByteHeapData(StateData state, Blackhole blackhole) {
    if ((state.srcHeapByteArray.length - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    final int upperBoundary = state.position + StateData.CHUNK_SZ;
    for (int idx = state.position; idx < upperBoundary; idx++) {
      final byte value = state.srcHeapByteArray[idx];
      blackhole.consume(value);
    }
    state.position = upperBoundary;
  }

  // --------------------------------------------------------------------------
  // Single byte write tests
  // --------------------------------------------------------------------------

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void setByteDirectData(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    final byte value        = state.seedData[0];
    final int upperBoundary = state.position + StateData.CHUNK_SZ;

    for (int idx = state.position; idx < upperBoundary; idx++) {
      MemoryUtils.putByte(state.tgtDirectBuffer + idx, value);
      blackhole.consume(state.tgtDirectBuffer);
    }
    state.position = upperBoundary;
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void setByteDirectDataNetty(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    final byte value        = state.seedData[0];
    final int upperBoundary = state.position + StateData.CHUNK_SZ;

    for (int idx = state.position; idx < upperBoundary; idx++) {
      state.tgtDirectBufferNetty.setByte(idx, value);
      blackhole.consume(state.tgtDirectBufferNetty);
    }
    state.position = upperBoundary;
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void setByteDirectDataDrill(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    final byte value        = state.seedData[0];
    final int upperBoundary = state.position + StateData.CHUNK_SZ;

    for (int idx = state.position; idx < upperBoundary;idx++) {
      state.tgtDirectBufferDrill.setByte(idx, value);
      blackhole.consume(state.tgtDirectBufferDrill);
    }
    state.position = upperBoundary;
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void setByteHeapData(StateData state, Blackhole blackhole) {
    if ((state.srcHeapByteArray.length - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    final byte value        = state.seedData[0];
    final int upperBoundary = state.position + StateData.CHUNK_SZ;

    for (int idx = state.position; idx < upperBoundary; idx++) {
      state.tgtHeapByteArray[idx] = value;
      blackhole.consume(state.tgtHeapByteArray);
    }
    state.position = upperBoundary;
  }

  // --------------------------------------------------------------------------
  // Single integer read tests
  // --------------------------------------------------------------------------

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void getIntDirectData(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    final int upperBoundary = state.position + StateData.CHUNK_SZ;

    for (int idx = state.position; idx < upperBoundary; idx += MemoryUtils.INT_NUM_BYTES) {
      final int value = MemoryUtils.getInt(state.srcDirectBuffer + idx);
      blackhole.consume(value);
    }
    state.position = upperBoundary;
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void getIntDirectDataNetty(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    final int upperBoundary = state.position + StateData.CHUNK_SZ;

    for (int idx = state.position; idx < upperBoundary; idx += MemoryUtils.INT_NUM_BYTES) {
      final int value = state.srcDirectBufferNetty.getInt(idx);
      blackhole.consume(value);
    }
    state.position = upperBoundary;
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void getIntDirectDataDrill(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    final int upperBoundary = state.position + StateData.CHUNK_SZ;

    for (int idx = state.position; idx < upperBoundary; idx += MemoryUtils.INT_NUM_BYTES) {
      final int value = state.srcDirectBufferDrill.getInt(idx);
      blackhole.consume(value);
    }
    state.position = upperBoundary;
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void getIntHeapData(StateData state, Blackhole blackhole) {
    if ((state.srcHeapIntArray.length - state.position) < (StateData.CHUNK_SZ / MemoryUtils.INT_NUM_BYTES)) {
      state.position = 0;
    }

    final int upperBoundary = state.position + (StateData.CHUNK_SZ / MemoryUtils.INT_NUM_BYTES);

    for (int idx = state.position; idx < upperBoundary; idx++) {
      final int value = state.srcHeapIntArray[idx];
      blackhole.consume(value);
    }
    state.position = upperBoundary;
  }

  // --------------------------------------------------------------------------
  // Single Integer write tests
  // --------------------------------------------------------------------------

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void setIntDirectData(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    final int value         = state.seedData[0];
    final int upperBoundary = state.position + StateData.CHUNK_SZ;

    for (int idx = state.position; idx < upperBoundary; idx += MemoryUtils.INT_NUM_BYTES) {
      MemoryUtils.putInt(state.tgtDirectBuffer + idx, value);
      blackhole.consume(state.tgtDirectBuffer);
    }
    state.position = upperBoundary;
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void setIntDirectDataNetty(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    final int value         = state.seedData[0];
    final int upperBoundary = state.position + StateData.CHUNK_SZ;

    for (int idx = state.position; idx < upperBoundary; idx += MemoryUtils.INT_NUM_BYTES) {
      state.tgtDirectBufferNetty.setInt(idx, value);
      blackhole.consume(state.tgtDirectBufferNetty);
    }
    state.position = upperBoundary;
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void setIntDirectDataDrill(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    final int value         = state.seedData[0];
    final int upperBoundary = state.position + StateData.CHUNK_SZ;

    for (int idx = state.position; idx < upperBoundary;idx += MemoryUtils.INT_NUM_BYTES) {
      state.tgtDirectBufferDrill.setInt(idx, value);
      blackhole.consume(state.tgtDirectBufferDrill);
    }
    state.position = upperBoundary;
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void setIntHeapData(StateData state, Blackhole blackhole) {
    if ((state.tgtHeapIntArray.length - state.position) < (StateData.CHUNK_SZ / MemoryUtils.INT_NUM_BYTES)) {
      state.position = 0;
    }

    final int value         = state.seedData[0];
    final int upperBoundary = state.position + (StateData.CHUNK_SZ / MemoryUtils.INT_NUM_BYTES);

    for (int idx = state.position; idx < upperBoundary; idx ++) {
      state.tgtHeapIntArray[idx] = value;
      blackhole.consume(state.tgtHeapIntArray);
    }
    state.position = upperBoundary;
  }

  // --------------------------------------------------------------------------
  // Single long read tests
  // --------------------------------------------------------------------------

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void getLongDirectData(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    final int upperBoundary = state.position + StateData.CHUNK_SZ;

    for (int idx = state.position; idx < upperBoundary; idx += MemoryUtils.LONG_NUM_BYTES) {
      final long value = MemoryUtils.getLong(state.srcDirectBuffer + idx);
      blackhole.consume(value);
    }
    state.position = upperBoundary;
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void getLongDirectDataNetty(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    final int upperBoundary = state.position + StateData.CHUNK_SZ;

    for (int idx = state.position; idx < upperBoundary; idx += MemoryUtils.LONG_NUM_BYTES) {
      final long value = state.srcDirectBufferNetty.getLong(idx);
      blackhole.consume(value);
    }
    state.position = upperBoundary;
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void getLongDirectDataDrill(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    final int upperBoundary = state.position + StateData.CHUNK_SZ;

    for (int idx = state.position; idx < upperBoundary; idx += MemoryUtils.LONG_NUM_BYTES) {
      final long value = state.srcDirectBufferDrill.getLong(idx);
      blackhole.consume(value);
    }
    state.position = upperBoundary;
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void getLongHeapData(StateData state, Blackhole blackhole) {
    if ((state.srcHeapLongArray.length - state.position) < (StateData.CHUNK_SZ / MemoryUtils.LONG_NUM_BYTES)) {
      state.position = 0;
    }

    final int upperBoundary = state.position + (StateData.CHUNK_SZ / MemoryUtils.LONG_NUM_BYTES);

    for (int idx = state.position; idx < upperBoundary; idx++) {
      final long value = state.srcHeapLongArray[idx];
      blackhole.consume(value);
    }
    state.position = upperBoundary;
  }

  // --------------------------------------------------------------------------
  // Single Long write tests
  // --------------------------------------------------------------------------

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void setLongDirectData(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    final long value        = state.seedData[0];
    final int upperBoundary = state.position + StateData.CHUNK_SZ;

    for (int idx = state.position; idx < upperBoundary; idx += MemoryUtils.LONG_NUM_BYTES) {
      MemoryUtils.putLong(state.tgtDirectBuffer + idx, value);
      blackhole.consume(state.tgtDirectBuffer);
    }
    state.position = upperBoundary;
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void setLongDirectDataNetty(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    final long value        = state.seedData[0];
    final int upperBoundary = state.position + StateData.CHUNK_SZ;

    for (int idx = state.position; idx < upperBoundary; idx += MemoryUtils.LONG_NUM_BYTES) {
      state.tgtDirectBufferNetty.setLong(idx, value);
      blackhole.consume(state.tgtDirectBufferNetty);
    }
    state.position = upperBoundary;
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void setLongDirectDataDrill(StateData state, Blackhole blackhole) {
    if ((StateData.DATA_SZ - state.position) < StateData.CHUNK_SZ) {
      state.position = 0;
    }

    final long value        = state.seedData[0];
    final int upperBoundary = state.position + StateData.CHUNK_SZ;

    for (int idx = state.position; idx < upperBoundary;idx += MemoryUtils.LONG_NUM_BYTES) {
      state.tgtDirectBufferDrill.setLong(idx, value);
      blackhole.consume(state.tgtDirectBufferDrill);
    }
    state.position = upperBoundary;
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void setLongHeapData(StateData state, Blackhole blackhole) {
    if ((state.tgtHeapLongArray.length - state.position) < (StateData.CHUNK_SZ / MemoryUtils.LONG_NUM_BYTES)) {
      state.position = 0;
    }

    final int value         = state.seedData[0];
    final int upperBoundary = state.position + (StateData.CHUNK_SZ / MemoryUtils.LONG_NUM_BYTES);

    for (int idx = state.position; idx < upperBoundary; idx++) {
      state.tgtHeapLongArray[idx] = value;
      blackhole.consume(state.tgtHeapLongArray);
    }
    state.position = upperBoundary;
  }
}
