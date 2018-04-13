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

import org.openjdk.jmh.infra.Blackhole;

import org.apache.drill.jmh.common.state.ByteMemoryAccessorState;
import org.apache.drill.jmh.common.utils.JMHUtils;
import org.apache.drill.jmh.common.utils.JMHUtils.JMHLaunchContext;
import org.apache.drill.jmh.common.utils.JMHUtils.JMHLaunchContextBuilder;

// Test Characteristics -
// - Input         : fixed length
// - Processing    : invoke the basic memory access APIs
// - Data Structure:
//   a) Direct memory using UNSAFE, Netty, and DrillBuf
//   b) Java heap arrays

public class BasicMemoryAccessBenchmark extends BasicMemoryAccessBaseTests {

  public static void main(String... args) throws Exception {
    JMHLaunchContextBuilder builder = new JMHLaunchContextBuilder(args);
    builder.addTestFilter(BasicMemoryAccessBenchmark.class.getName());
    JMHLaunchContext launchContext  = builder.build();

    if (launchContext.testMode) {
      test();
    } else if (launchContext.opts != null) {
      new org.openjdk.jmh.runner.Runner(launchContext.opts).run();
    }
  }

  public static void test() {
    StateData state                      = new StateData();
    BasicMemoryAccessBenchmark benchmark = new BasicMemoryAccessBenchmark();
    Blackhole blackHole                  = JMHUtils.newBlackhole();
    final String loggingPrefix           = "*** ";

    state.doSetup();

    //-------------------------------------------------------------------------
    // READ SINGLE BYTE TESTS
    // ------------------------------------------------------------------------

    StateData.getLogger().info(String.format("%sRunning the Single Read Byte tests..", loggingPrefix));

    // Direct Memory benchmark
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      benchmark.getByteDirectData(state, blackHole);
    }

    // Direct Memory benchmark Netty
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      benchmark.getByteDirectDataNetty(state, blackHole);
    }

    // Direct Memory benchmark Drill
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      benchmark.getByteDirectDataDrill(state, blackHole);
    }

    // Heap Memory benchmark
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      benchmark.getByteHeapData(state, blackHole);
    }

    //-------------------------------------------------------------------------
    // WRITE SINGLE BYTE TESTS
    // ------------------------------------------------------------------------

    StateData.getLogger().info(String.format("%sRunning the Single Write Byte tests..", loggingPrefix));

    // Direct Memory benchmark
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      benchmark.setByteDirectData(state, blackHole);
    }
    ByteMemoryAccessorState.printDMData(state);

    // Direct Memory benchmark Netty
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      benchmark.setByteDirectDataNetty(state, blackHole);
    }
    ByteMemoryAccessorState.printDMDataNetty(state);

    // Direct Memory benchmark Drill
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      benchmark.setByteDirectDataDrill(state, blackHole);
    }
    ByteMemoryAccessorState.printDMDataDrill(state);

    // Heap Memory benchmark
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      benchmark.setByteHeapData(state, blackHole);
    }
    ByteMemoryAccessorState.printHeapData(state);

    //-------------------------------------------------------------------------
    // READ SINGLE INTEGER TESTS
    // ------------------------------------------------------------------------

    StateData.getLogger().info(String.format("%sRunning the Single Read Integer tests..", loggingPrefix));

    // Direct Memory benchmark
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      benchmark.getIntDirectData(state, blackHole);
    }

    // Direct Memory benchmark Netty
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      benchmark.getIntDirectDataNetty(state, blackHole);
    }

    // Direct Memory benchmark Drill
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      benchmark.getIntDirectDataDrill(state, blackHole);
    }

    // Heap Memory benchmark
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      benchmark.getIntHeapData(state, blackHole);
    }

    //-------------------------------------------------------------------------
    // WRITE SINGLE INTEGER TESTS
    // ------------------------------------------------------------------------

    StateData.getLogger().info(String.format("%sRunning the Single Write Integer tests..", loggingPrefix));

    // Direct Memory benchmark
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      benchmark.setIntDirectData(state, blackHole);
    }
    ByteMemoryAccessorState.printDMData(state);

    // Direct Memory benchmark Netty
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      benchmark.setIntDirectDataNetty(state, blackHole);
    }
    ByteMemoryAccessorState.printDMDataNetty(state);

    // Direct Memory benchmark Drill
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      benchmark.setIntDirectDataDrill(state, blackHole);
    }
    ByteMemoryAccessorState.printDMDataDrill(state);

    // Heap Memory benchmark
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      benchmark.setIntHeapData(state, blackHole);
    }
    ByteMemoryAccessorState.printHeapData(state);

    //-------------------------------------------------------------------------
    // READ SINGLE LONG TESTS
    // ------------------------------------------------------------------------

    StateData.getLogger().info(String.format("%sRunning the Single Read Long tests..", loggingPrefix));

    // Direct Memory benchmark
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      benchmark.getLongDirectData(state, blackHole);
    }

    // Direct Memory benchmark Netty
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      benchmark.getLongDirectDataNetty(state, blackHole);
    }

    // Direct Memory benchmark Drill
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      benchmark.getLongDirectDataDrill(state, blackHole);
    }

    // Heap Memory benchmark
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      benchmark.getLongHeapData(state, blackHole);
    }

    //-------------------------------------------------------------------------
    // WRITE SINGLE LONG TESTS
    // ------------------------------------------------------------------------

    StateData.getLogger().info(String.format("%sRunning the Single Write Long tests..", loggingPrefix));

    // Direct Memory benchmark
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      benchmark.setLongDirectData(state, blackHole);
    }
    ByteMemoryAccessorState.printDMData(state);

    // Direct Memory benchmark Netty
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      benchmark.setLongDirectDataNetty(state, blackHole);
    }
    ByteMemoryAccessorState.printDMDataNetty(state);

    // Direct Memory benchmark Drill
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      benchmark.setLongDirectDataDrill(state, blackHole);
    }
    ByteMemoryAccessorState.printDMDataDrill(state);

    // Heap Memory benchmark
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      benchmark.setLongHeapData(state, blackHole);
    }
    ByteMemoryAccessorState.printHeapData(state);


  }
}
