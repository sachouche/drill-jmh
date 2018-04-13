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
package org.apache.drill.jmh;

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

import org.apache.drill.jmh.common.state.ParquetReaderState;
import org.apache.drill.jmh.common.utils.JMHUtils;
import org.apache.drill.jmh.common.utils.JMHUtils.JMHLaunchContext;
import org.apache.drill.jmh.common.utils.JMHUtils.JMHLaunchContextBuilder;

// Test Characteristics -
// - Input         : fixed and variable length
// - Processing    : implement read next Parquet batch
// - Algorithms
//   a) Columnar Parquet reader which uses DrillBufs
//   b) Columnar Parquet reader which operates in a bulk fashion using
//      Java byte arrays as intermediary buffers; source and results are
//      loaded in DrillBufs

public class ParquetReaderBenchmark {

  @State(Scope.Thread)
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public static class StateData extends ParquetReaderState {

    @Setup(Level.Trial)
    public void doSetup() {
      super.doSetup();
    }

    @TearDown(Level.Trial)
    public void doTearDown() {
      super.doTearDown();
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void readNextBatchColumnar(StateData state, Blackhole blackhole) {
    if (state.pageReader.allDataRead()) {
      state.pageReader.reset();
    }
    state.columnarReader.readBatch();

    blackhole.consume(state.columnarReader);
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public void readNextBatchBulkColumnar(StateData state, Blackhole blackhole) {
    if (state.pageReader.allDataRead()) {
      state.pageReader.reset();
    }
    state.bulkColumnarReader.readBatch();

    blackhole.consume(state.bulkColumnarReader);
  }

  public static void main(String... args) throws Exception {
    JMHLaunchContextBuilder builder = new JMHLaunchContextBuilder(args);
    builder.addParquetOptions(); // add custom options

    JMHLaunchContext launchContext = builder.build();

    if (launchContext.testMode) {
      test();
    } else if (launchContext.opts != null) {
      new org.openjdk.jmh.runner.Runner(launchContext.opts).run();
    }
  }

  private static void test() {
    StateData stateData               = new StateData();
    ParquetReaderBenchmark benchmark = new ParquetReaderBenchmark();
    Blackhole blackHole               = JMHUtils.newBlackhole();

    stateData.doSetup();
    benchmark.readNextBatchColumnar(stateData, blackHole);
    StateData.printVVData(stateData.columnarReader, 1024, 16);

    stateData.pageReader.reset();
    for (int idx = 0; idx < 102400; idx++) {
      benchmark.readNextBatchBulkColumnar(stateData, blackHole);
    }
    StateData.printVVData(stateData.bulkColumnarReader, 1024, 16);
  }

}
