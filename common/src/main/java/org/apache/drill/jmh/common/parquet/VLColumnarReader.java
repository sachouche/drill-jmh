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
package org.apache.drill.jmh.common.parquet;

import org.apache.drill.exec.memory.BaseAllocator;

/** Columnar parquet reader */
public final class VLColumnarReader extends AbstractVLColumnarReader {
  public final PageReader pageReader;
  public int numReadValues;
  public int dataOffset;
  public int totalLength;

  public VLColumnarReader(int batchSize,
    int dataCapacity, BaseAllocator allocator, PageReader pageReader) {
    super(batchSize, dataCapacity, allocator);

    this.pageReader = pageReader;
  }

  @Override
  public int readBatch() {
    next();
    return vv.numValues;
  }

  private void next() {
    numReadValues = 0;
    dataOffset    = 0;
    totalLength   = 0;

    while( numReadValues < batchSize) {
      if (readValue()) {
        ++numReadValues;
      } else {
        pageReader.reset();
      }
    }
    vv.setValueCapacity(numReadValues);
  }

  private boolean readValue() {
    // We need to ensure there is a page of data to be read
    if (!pageReader.hasPage() || pageReader.currentPageCount == pageReader.pageValuesRead) {
      if (pageReader.allDataRead() || !pageReader.next()) {
        return false;
      }
    }

    // Read current entry length
    int len = pageReader.pageData.getInt(pageReader.pageReadyToReadPosInBytes);
    pageReader.pageReadyToReadPosInBytes += 4;

    // Update the total length
    totalLength += len;

    // Update the VV offset
    vv.offsets.setInt((numReadValues + 1) * 4, totalLength);

    // Write the data
    vv.data.setBytes(dataOffset, pageReader.pageData, pageReader.pageReadyToReadPosInBytes, len);
    dataOffset                           += len;
    pageReader.pageReadyToReadPosInBytes += len;

    // Update few page reader counters
    ++pageReader.pageValuesRead;
    ++pageReader.totalValuesRead;

    return true;
  }
}
