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
// - Input         : fixed & variable length data (configurable)
// - Processing    : bulk getter and setter APIs
// - Data Structure:
//   a) Direct memory using UNSAFE, Netty, and DrillBuf
//   b) Java heap arrays
public class BulkMemoryAccessBenchmark extends BulkMemoryAccessBaseTests {

  public static void main(String... args) throws Exception {
    JMHLaunchContextBuilder builder = new JMHLaunchContextBuilder(args);
    builder.addTestFilter(BulkMemoryAccessBenchmark.class.getName());
    builder.addMemoryBulkOptions(); // add custom options
    JMHLaunchContext launchContext  = builder.build();

    if (launchContext.testMode) {
      test();
    } else if (launchContext.opts != null) {
      new org.openjdk.jmh.runner.Runner(launchContext.opts).run();
    }
  }

  private static void test() {
    StateData stateData                 = new StateData();
    BulkMemoryAccessBenchmark benchmark = new BulkMemoryAccessBenchmark();
    Blackhole blackHole                 = JMHUtils.newBlackhole();
    final String loggingPrefix          = "*** ";

    stateData.doSetup();

    //-------------------------------------------------------------------------
    // COPY BYTES TESTS
    // ------------------------------------------------------------------------

    StateData.getLogger().info(String.format("%sRunning the Copy Bytes tests..", loggingPrefix));

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

    // run the Heap Memory benchmark
    for (int idx = 0; idx < StateData.DATA_SZ; idx += StateData.CHUNK_SZ) {
      benchmark.getBytesHeapData(stateData, blackHole);
    }
    ByteMemoryAccessorState.printHeapData(stateData);


  }

}
