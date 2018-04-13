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

/** Bulk columnar parquet reader */
public final class VLBulkColumnarReader extends AbstractVLColumnarReader {
  public final PageReader pageReader;
  public int numReadValues;
  private VLBinaryColumnBulkInput bulkInput;


  public VLBulkColumnarReader(int batchSize,
    int dataCapacity, BaseAllocator allocator, PageReader pageReader) {
    super(batchSize, dataCapacity, allocator);

    this.pageReader = pageReader;
    this.bulkInput  = new VLBinaryColumnBulkInput(pageReader);
  }

  public void reset() {
    // NOOP
  }

  @Override
  public int readBatch() {
    next();
    return vv.numValues;
  }

  private void next() {
    vv.reset();
    bulkInput.newBatch(batchSize);

    numReadValues = 0;

    while (numReadValues < batchSize) {
      if (bulkInput.hasNext()) {
        final BulkEntry entry = bulkInput.next();
        vv.setSafe(entry); // load the bulk entry
        numReadValues += entry.getNumValues();

      } else {
        pageReader.reset();
        bulkInput.newBatch(batchSize - numReadValues);
      }
    }

    bulkInput.done();
    vv.setValueCapacity(numReadValues);
  }
}
