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

import java.util.Random;

import org.apache.drill.exec.memory.BaseAllocator;

import io.netty.buffer.ByteBuf;

/** PageReader emulator */
public class PageReader {

  final int pageSize;
  final BaseAllocator allocator;
  final boolean isFixedSize;
  ByteBuf pageData;
  int pageValuesRead;
  int pageReadyToReadPosInBytes;
  int currentPageCount; // total values per page
  int pageByteCount;
  final long totalValuesToRead;
  long totalValuesRead;
  long numPagesToRead;
  long remainingPages;
  boolean hasPage;
  int expDataSize;

  public PageReader(long _payloadSize, int _pageSize, int _expDataSize, boolean _isFixedSize, BaseAllocator allocator) {
    this.allocator       = allocator;
    this.pageSize        = _pageSize;
    this.isFixedSize     = _isFixedSize;
    this.expDataSize     = _expDataSize;
    this.numPagesToRead  = (long) Math.ceil( ((double)_payloadSize / pageSize));
    this.remainingPages  = this.numPagesToRead;

    // load the page data once
    if (isFixedSize) {
      fixedLengthload();

    } else {
      vlLengthload();
    }

    this.totalValuesToRead = currentPageCount * remainingPages;
    this.totalValuesRead   = 0;
  }

  boolean hasPage() {
    return hasPage;
  }

  public int getNumEntriesPerPage() {
    return currentPageCount;
  }

  public boolean isFixedSize() {
    return isFixedSize;
  }

  public boolean allDataRead() {
    return totalValuesToRead == totalValuesRead;
  }

  public void reset() {
    hasPage         = false;
    remainingPages  = numPagesToRead;
    totalValuesRead = 0;
    pageValuesRead  = 0;
  }

  public void close() {
    pageData.release();
    pageData = null;
  }

  private void vlLengthload() {
    pageData      = allocator.buffer(pageSize);
    Random rand   = new Random();
    int pos       = 0;
    byte data     = 'a';

    while (true) {
      final int entry_len = rand.nextInt(expDataSize) + 1;
      final int entry_sz  = 4 + entry_len;
      final int remaining = pageSize - pos;

      if (remaining < entry_sz) {
        break;
      }

      pageData.setInt(pos, entry_len); pos += 4;

      final int upperBound = pos + entry_len;
      for ( ; pos < upperBound; ++pos) {
        pageData.setByte(pos, data);
        data = nextByte(data);
      }

      // Update page info
      ++currentPageCount;
      pageByteCount += entry_sz;
    }
  }

  private void fixedLengthload() {
    currentPageCount = pageSize / (4 + expDataSize);
    pageByteCount    = currentPageCount * (4 + expDataSize);
    pageData         = allocator.buffer(pageByteCount);
    int pos          = 0;
    byte data        = 'a';

    for (int idx = 0; idx < currentPageCount; idx++) {
      pageData.setInt(pos, expDataSize); pos += 4;

      final int upperBound = pos + expDataSize;
      for ( ; pos < upperBound; ++pos) {
        pageData.setByte(pos, data);
        data = nextByte(data);
      }
    }
  }

  private byte nextByte(byte current) {
    return (byte) (current < 122 ? current + 1 : 97);
  }

  boolean next() {
    if (remainingPages-- > 0) {
      pageValuesRead            = 0;
      pageReadyToReadPosInBytes = 0;
      hasPage                   = true;

    } else {
      hasPage = false;
    }
    return hasPage;
  }
}
