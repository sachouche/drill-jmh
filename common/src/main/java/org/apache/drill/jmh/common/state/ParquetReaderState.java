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

import org.apache.drill.exec.memory.RootAllocator;
import org.apache.drill.jmh.common.parquet.AbstractVLColumnarReader;
import org.apache.drill.jmh.common.parquet.PageReader;
import org.apache.drill.jmh.common.parquet.VLBulkColumnarReader;
import org.apache.drill.jmh.common.parquet.VLColumnarReader;
import org.apache.drill.jmh.common.utils.JMHUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetReaderState {
  private static final Logger logger = LoggerFactory.getLogger(ParquetReaderState.class.getCanonicalName());

  // Hard coded constants
  public static final long PAYLOAD_SIZE    = 1 << 34; // 16GB of payload
  public static final int PAGE_SIZE        = 1 << 24; // 16MB page size to ensure data cannot be fully cached
  public static final int BATCH_SIZE       = 1 << 15; // 32k

  // Configurable constants
  public static int ENTRY_SIZE       = JMHUtils.DEFAULT_PARQUET_ENTRY_LEN;
  public static boolean IS_FIXED_LEN = true;

  public RootAllocator drillRootAllocator;
  public PageReader pageReader;
  public VLColumnarReader columnarReader;
  public VLBulkColumnarReader bulkColumnarReader;

  public void doSetup() {
    initStateFromEnv();

    drillRootAllocator = new RootAllocator(PAGE_SIZE * 4);
    pageReader         = new PageReader(PAYLOAD_SIZE, PAGE_SIZE, ENTRY_SIZE, IS_FIXED_LEN, drillRootAllocator);
    columnarReader     = new VLColumnarReader(BATCH_SIZE, PAGE_SIZE, drillRootAllocator, pageReader);
    bulkColumnarReader = new VLBulkColumnarReader(BATCH_SIZE, PAGE_SIZE, drillRootAllocator, pageReader);

    logger.info(String.format("%n\tnum-entries-per-page: [%d]%n\tpage-size: [%d]%n\tfixed-size: [%s]%n\tentry-len: [%d]%n",
      pageReader.getNumEntriesPerPage(),
      PAGE_SIZE,
      IS_FIXED_LEN ? "fixed-length" : "variable-length",
      ENTRY_SIZE
      )
    );
  }

  public void doTearDown() {
    if (pageReader != null) {
      pageReader.close();
      pageReader = null;
    }
    if (columnarReader != null) {
      columnarReader.close();
      columnarReader = null;
    }
    if (bulkColumnarReader != null) {
      bulkColumnarReader.close();
      bulkColumnarReader = null;
    }
    if (drillRootAllocator != null) {
      drillRootAllocator.close();
      drillRootAllocator = null;
    }
  }

  public static void printVVData(AbstractVLColumnarReader reader, int startIdx, int len) {
    StringBuilder msg =
      new StringBuilder(String.format("Dumping Value Vector data (%s):\n", reader.getClass().getName()));

    for (int idx = startIdx; idx < (startIdx + len); idx++) {
      msg.append(String.format("\tentry-idx: [%d], entry-len: [%d], entry-data: [%s]%n",
        idx, reader.vv.getEntryLen(idx), reader.vv.getEntryData(idx))
      );
    }
    logger.info(msg.toString());
  }

  private static void initStateFromEnv() {
    final String isVariableLenStr = System.getProperty(JMHUtils.PARQUET_VAR_LEN_JVM_OPTION);
    final String entryLenStr      = System.getProperty(JMHUtils.PARQUET_ENTRY_LEN_JVM_OPTION);

    if (isVariableLenStr != null && "true".equalsIgnoreCase(isVariableLenStr)) {
      IS_FIXED_LEN = false;
    }

    if (entryLenStr != null) {
      try {
        ENTRY_SIZE = Integer.parseInt(entryLenStr);
      } catch (Exception e) {
        // NOOP
      }
    }
  }

}
