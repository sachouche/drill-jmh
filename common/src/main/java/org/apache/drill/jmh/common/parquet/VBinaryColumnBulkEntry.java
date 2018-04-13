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

import io.netty.buffer.DrillBuf;

/** Implements the {@link VarBinaryVector.BulkInput} interface to optimize data copy */
final class VBinaryColumnBulkEntry implements BulkEntry {
  private final int expectedDataLen;
  private int startOffset;
  private int totalLength;
  private int numValues;

  private final int[] lengths;
  private final byte[] data;

  public VBinaryColumnBulkEntry(boolean isFixedSize, int _expectedDataLen) {
    this.expectedDataLen = _expectedDataLen;
    int lengthSz         = -1;
    int dataSz           = -1;

    if (isFixedSize) {
      final int max_num_values    = VLBulkPageReader.BUFF_SZ / (4 + expectedDataLen);
      lengthSz                    = max_num_values;
      dataSz                      = max_num_values * expectedDataLen + VLBulkPageReader.PADDING;

    } else {
      // For variable length data, we need to handle a) maximum number of entries and b) max entry length
      final int smallest_data_len = 1;
      final int largest_data_len  = VLBulkPageReader.BUFF_SZ - 4;
      final int max_num_values    = VLBulkPageReader.BUFF_SZ / (4 + smallest_data_len);
      lengthSz                    = max_num_values;
      dataSz                      = largest_data_len + VLBulkPageReader.PADDING;
    }

    lengths = new int[lengthSz];
    data    = new byte[dataSz];
  }
  /** @inheritDoc */
  @Override
  public int getTotalLength() {
    return totalLength;
  }

  /** @inheritDoc */
  @Override
  public boolean arrayBacked() {
    return true;
  }

  /** @inheritDoc */
  @Override
  public byte[] getArrayData() {
    return data;
  }

  /** @inheritDoc */
  @Override
  public DrillBuf getData() {
    throw new RuntimeException("Unsupported operation..");
  }

  /** @inheritDoc */
  @Override
  public int getDataStartOffset() {
    return startOffset;
  }

  /** @inheritDoc */
  @Override
  public int[] getValuesLength() {
    return lengths;
  }

  /** @inheritDoc */
  @Override
  public int getNumValues() {
    return numValues;
  }

  @Override
  public boolean hasNulls() {
    return false;
  }

  void set(int _startOffset, int _totalLength, int _numValues) {
    startOffset      = _startOffset;
    totalLength      = _totalLength;
    numValues        = _numValues;
  }

  int getMaxEntries() {
    return lengths.length;
  }
}