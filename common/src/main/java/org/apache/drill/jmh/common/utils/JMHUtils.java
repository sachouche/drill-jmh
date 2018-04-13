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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.openjdk.jmh.infra.Blackhole;

public final class JMHUtils {

  // Default options
  private static final String TEST_MODE_OPTION             = "t";
  private static final String TEST_FILTER_OPTION           = "f";
  private static final String BOUNDARY_CHECKS_OPTION       = "b";
  private static final String REFERENCE_COUNT_OPTION       = "r";
  private static final String WARMUP_ITERATION_OPTION      = "w";
  private static final String MEASURE_ITERATION_OPTION     = "m";
  private static final String HELP_OPTION                  = "h";

  // Custom options (Bulk Memory)
  private static final String MEMORY_ACCESS_VAR_LEN_OPTION    = "V";
  private static final String MEMORY_ACCESS_LEN_OPTION        = "L";
  public static final String MEMORY_ACCESS_LEN_JVM_OPTION     = "drill.jmh.memory.bulk.entry_len";
  public static final String MEMORY_ACCESS_VAR_LEN_JVM_OPTION = "drill.jmh.memory.bulk.varlen";

  // Custom options (Parquet)
  private static final String PARQUET_VAR_LEN_OPTION       = "V";
  private static final String PARQUET_ENTRY_LEN_OPTION     = "L";
  public static final String PARQUET_VAR_LEN_JVM_OPTION    = "drill.jmh.parquet.varlen";
  public static final String PARQUET_ENTRY_LEN_JVM_OPTION  = "drill.jmh.parquet.entry_len";

  // Default values
  private static final int DEFAULT_WARMUP_ITER             = 5;
  private static final int DEFAULT_MEASURE_ITER            = 10;
  public static final int DEFAULT_PARQUET_ENTRY_LEN        = 1;
  public static final int DEFAULT_MEMORY_ACCESS_LEN        = 1;
  public static final String DEFAULT_TEST_FILTER           = ".*";


  /** JMH run context class */
  public static final class JMHLaunchContext {
    public org.openjdk.jmh.runner.options.Options opts;
    public boolean testMode;
  }

  /** Builder class */
  public static final class JMHLaunchContextBuilder {
    private final String[] args;
    private final JMHLaunchContext launchContext = new JMHLaunchContext();
    private final Options options                = initDefaultOptions();
    private final List<String> jvmArgs           = loadDefaultJVMArgs();
    private String testFilter                    = DEFAULT_TEST_FILTER;
    private int numWarmupIter                    = DEFAULT_WARMUP_ITER;
    private int numMeasureIter                   = DEFAULT_MEASURE_ITER;

    // Custom tests
    private boolean addedBulkMemoryOptions;
    private boolean addedParquetOptions;

    public JMHLaunchContextBuilder(String[] args) {
      this.args = args;
    }

    public JMHLaunchContextBuilder addParquetOptions() {
      JMHUtils.addParquetOptions(options);
      addedParquetOptions = true;

      return this;
    }

    public JMHLaunchContextBuilder addMemoryBulkOptions() {
      JMHUtils.addBulkMemoryOptions(options);
      addedBulkMemoryOptions = true;

      return this;
    }

    public JMHLaunchContextBuilder addTestFilter(String filter) {
      testFilter = filter;

      return this;
    }

    public JMHLaunchContext build() throws Exception {
      // Parse the command line arguments
      CommandLineParser parser = new DefaultParser();
      CommandLine cmd = parser.parse( options, args);

      if (cmd.hasOption(HELP_OPTION)) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( "jmh-tests", options );
        return launchContext; // NOOP
      }

      if (cmd.hasOption(TEST_MODE_OPTION)) {
        launchContext.testMode = true;
      }

      if (cmd.hasOption(TEST_FILTER_OPTION)) {
        testFilter = cmd.getOptionValue(TEST_FILTER_OPTION, testFilter);
      }

      // NOTES
      // - io.netty.buffer.bytebuf.checkAccessible turns off Netty & Drill reference count checks
      // - Reference count checks have to be explicitly turned off as the default is on


      if (cmd.hasOption(BOUNDARY_CHECKS_OPTION)) {
        jvmArgs.add("-Ddrill.exec.memory.enable_unsafe_bounds_check=true");
      } else {
        jvmArgs.add("-Ddrill.exec.memory.enable_unsafe_bounds_check=false");
      }

      if (cmd.hasOption(REFERENCE_COUNT_OPTION)) {
        jvmArgs.add("-Dio.netty.buffer.bytebuf.checkAccessible=true");
      } else {
        jvmArgs.add("-Dio.netty.buffer.bytebuf.checkAccessible=false");
      }

      if (cmd.hasOption(WARMUP_ITERATION_OPTION)) {
        numWarmupIter = Integer.parseInt(cmd.getOptionValue(WARMUP_ITERATION_OPTION));
      }

      if (cmd.hasOption(MEASURE_ITERATION_OPTION)) {
        numMeasureIter = Integer.parseInt(cmd.getOptionValue(MEASURE_ITERATION_OPTION));
      }

      if (addedParquetOptions) {
        if (cmd.hasOption(PARQUET_VAR_LEN_OPTION)) {
          final String entry = String.format("-D%s=true", PARQUET_VAR_LEN_JVM_OPTION);
          jvmArgs.add(entry);
        }
        if (cmd.hasOption(PARQUET_ENTRY_LEN_OPTION)) {
          final int entryLength = Integer.parseInt(cmd.getOptionValue(PARQUET_ENTRY_LEN_OPTION));
          final String entry    = String.format("-D%s=%d", PARQUET_ENTRY_LEN_JVM_OPTION, entryLength);
          jvmArgs.add(entry);
        }
      }

      if (addedBulkMemoryOptions) {
        if (cmd.hasOption(MEMORY_ACCESS_VAR_LEN_OPTION)) {
          final String entry = String.format("-D%s=true", MEMORY_ACCESS_VAR_LEN_JVM_OPTION);
          jvmArgs.add(entry);
        }
        if (cmd.hasOption(MEMORY_ACCESS_LEN_OPTION)) {
          final int entryLength = Integer.parseInt(cmd.getOptionValue(MEMORY_ACCESS_LEN_OPTION));
          final String entry    = String.format("-D%s=%d", MEMORY_ACCESS_LEN_JVM_OPTION, entryLength);
          jvmArgs.add(entry);
        }
      }

      if (!launchContext.testMode) {
        org.openjdk.jmh.runner.options.Options opts = new org.openjdk.jmh.runner.options.OptionsBuilder()
            .include(testFilter)
            .warmupIterations(numWarmupIter)
            .measurementIterations(numMeasureIter)
            .jvmArgs(jvmArgs.toArray(new String[jvmArgs.size()]))
            .shouldDoGC(true)
            .forks(1)
            .build();

        launchContext.opts = opts;
      }
      return launchContext;

    }
  }

  /**
   * @return new black hole instance
   */
  public static Blackhole newBlackhole() {
    return new Blackhole("Today's password is swordfish. I understand instantiating Blackholes directly is dangerous.");
  }


  // --------------------------------------------------------------------------
  // Private implementation
  // --------------------------------------------------------------------------

  private static Options initDefaultOptions() {
    final Options options = new Options();

    options.addOption(HELP_OPTION,              false, "display this help");
    options.addOption(TEST_FILTER_OPTION,       true,  String.format("JMH test filter (default: %s)",  DEFAULT_TEST_FILTER));
    options.addOption(TEST_MODE_OPTION,         false, "run single iteration for testing");
    options.addOption(WARMUP_ITERATION_OPTION,  true,  String.format("Number of Warmup iterations (default: %d)",  DEFAULT_WARMUP_ITER));
    options.addOption(MEASURE_ITERATION_OPTION, true,  String.format("Number of Measure iterations (default: %d)", DEFAULT_MEASURE_ITER));
    options.addOption(BOUNDARY_CHECKS_OPTION,   false, "enable boundary checks");
    options.addOption(REFERENCE_COUNT_OPTION,   false, "enable reference count checks");


    return options;
  }

  private static List<String> loadDefaultJVMArgs() {
    final List<String> jvmArgs = new ArrayList<String>();

    jvmArgs.add("-server");
    jvmArgs.add("-Xms4g");
    jvmArgs.add("-Xmx4g");
    jvmArgs.add("-XX:+AggressiveOpts");
    return jvmArgs;
  }

  private static void addParquetOptions(Options options) {
    options.addOption(PARQUET_VAR_LEN_OPTION,   false, "enable variable length data (parquet tests)");
    options.addOption(PARQUET_ENTRY_LEN_OPTION, true ,  String.format("set the maximum entry length (default: %d)", DEFAULT_PARQUET_ENTRY_LEN));
  }

  private static void addBulkMemoryOptions(Options options) {
    options.addOption(MEMORY_ACCESS_VAR_LEN_OPTION, false, "enable variable length data");
    options.addOption(MEMORY_ACCESS_LEN_OPTION,     true,  String.format("set the memory access length (default: %d)", DEFAULT_MEMORY_ACCESS_LEN));
  }

  // Disabling object construction
  private JMHUtils() {
  }

}
