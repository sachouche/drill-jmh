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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.drill.jmh.common.utils.FastAccessUtils;
import org.apache.drill.jmh.common.utils.MemoryUtils;

import io.netty.buffer.ByteBuf;

public final class VLBulkPageReader {
  static final int BUFF_SZ = 1 << 12; // 4kb
  static final int PADDING = 0x08;
  /** byte buffer used for buffering page data */
  private final ByteBuffer buffer;
  /** Page data */
  private ByteBuf pageData;
  /** Offset within the page data */
  private int pageDataOff;
  /** page data length */
  private int pageDataLen;
  /** expected data len */
  final int expectedDataLen;
  /** Bulk entry */
  private final VBinaryColumnBulkEntry entry;
  /** Indicates whether we're emulating a fixed length column */
  private final boolean isFixedSize;

  public VLBulkPageReader(boolean isFixedSize,
      ByteBuf _pageData, int _startOff, int _pageDataLen, int _expectedDataLen) {

    this.isFixedSize       = isFixedSize;
    this.expectedDataLen = _expectedDataLen;
    this.entry             = new VBinaryColumnBulkEntry(isFixedSize, expectedDataLen);
    this.buffer            = ByteBuffer.allocate(BUFF_SZ + PADDING);

    // Set the buffer to the native byte order
    buffer.order(ByteOrder.nativeOrder());

    this.pageData    = _pageData;
    this.pageDataOff = _startOff;
    this.pageDataLen = _pageDataLen;
  }

  void set(ByteBuf _pageData, int _startOff, int _pageDataLen) {
    this.pageData    = _pageData;
    this.pageDataOff = _startOff;
    this.pageDataLen = _pageDataLen;
  }

  final VBinaryColumnBulkEntry getEntry(int valuesToRead) {
    if (isFixedSize) {
      return getFixedEntry(valuesToRead);

    } else {
      return getVLEntry(valuesToRead);
    }
  }

  private final VBinaryColumnBulkEntry getFixedEntry(int valuesToRead) {
    // Load the buffer from where we left off
    load(true);

    final int entry_sz        = 4 + expectedDataLen;
    final int max_values      = Math.min(entry.getMaxEntries(), (pageDataLen-pageDataOff) / entry_sz);
    final int read_batch      = Math.min(max_values, valuesToRead);
    final int[] value_lengths = entry.getValuesLength();
    final byte[] tgt_buff     = entry.getArrayData();
    final byte[] src_buff     = buffer.array();
    int idx                   = 0;

    for ( ; idx < read_batch; ++idx) {
        final int curr_pos = idx * entry_sz;
        final int data_len = getInt(src_buff, curr_pos);

        if (data_len != expectedDataLen) {
          return null;
        }

        value_lengths[idx] = data_len;
        final int tgt_pos  = idx * expectedDataLen;

        if (expectedDataLen > 0) {
          if (expectedDataLen <= 8) {
            FastAccessUtils.vlCopyLELong(src_buff, curr_pos + 4, tgt_buff, tgt_pos, data_len);
          } else {
            FastAccessUtils.vlCopyGTLong(src_buff, curr_pos + 4, tgt_buff, tgt_pos, data_len);
          }
        }

    } // for-loop

    // Update the page data buffer offset
    pageDataOff += idx * entry_sz;

    // Now set the bulk entry
    entry.set(0, idx * expectedDataLen, idx);

    return entry;
  }

  private final VBinaryColumnBulkEntry getVLEntry(int valuesToRead) {
    // Load the buffer if needed
    load(true);

    final int[] value_lengths = entry.getValuesLength();
    final int read_batch      = Math.min(entry.getMaxEntries(), valuesToRead);
    final byte[] tgt_buff     = entry.getArrayData();
    final byte[] src_buff     = buffer.array();
    final int src_len         = buffer.remaining();
    final int tgt_len         = BUFF_SZ;

    // Counters
    int numValues = 0;
    int tgt_pos   = 0;
    int src_pos   = 0;

    if (read_batch == 0) {
      throw new RuntimeException("read batch cannot be zero..");
    }

    for (; numValues < read_batch; ) {
      if (src_pos > src_len - 4) {
        break;
      }

      final int data_len = getInt(src_buff, src_pos);
      src_pos           += 4;

      if (src_len < (src_pos + data_len)
       || tgt_len < (tgt_pos + data_len)) {

        break;
      }

      value_lengths[numValues++] = data_len;

      if (data_len > 0) {
        if (data_len <= 8) {
          FastAccessUtils.vlCopyLELong(src_buff, src_pos, tgt_buff, tgt_pos, data_len);
        } else {
          FastAccessUtils.vlCopyGTLong(src_buff, src_pos, tgt_buff, tgt_pos, data_len);
        }
      }

      // Update the counters
      src_pos += data_len;
      tgt_pos += data_len;
    }

    // Update the page data buffer offset
    incPageDataOffset(numValues * 4 + tgt_pos);

    if (remainingPageData() < 0) {
      throw new RuntimeException("Page data offset [" + pageDataOff + "] is not valid..");
    }

    // Now set the bulk entry
    entry.set(0, tgt_pos, numValues);

    return entry;
  }

  /**
   * Loads new data into the buffer if empty or the force flag is set.
   *
   * @param force flag to force loading new data into the buffer
   */
  private final boolean load(boolean force) {

    if (!force && buffer.hasRemaining()) {
      return true; // NOOP
    }

    buffer.clear();
    final int bufferCapacity = buffer.capacity() - PADDING;

    int remaining = remainingPageData();
    int toCopy    = remaining > bufferCapacity ? bufferCapacity : remaining;

    if (toCopy == 0) {
      return false;
    }

    if (toCopy < 0 || toCopy > buffer.capacity()) {
      throw new RuntimeException("toCopy [" + toCopy + "] is not valid..");
    }

    pageData.getBytes(pageDataOff, buffer.array(), buffer.position(), toCopy);

    buffer.limit(toCopy);

    return true;
  }

  private final int remainingPageData() {
    return pageDataLen - pageDataOff;
  }

  private final void incPageDataOffset(int len) {
    pageDataOff += len; // indicates we consumed this piece of data
  }

  final boolean checkDataLen(int expectedDataLen) {
    if (buffer.remaining() < expectedDataLen) {
      return false;
    }
    return true;
  }

  private static final int getInt(byte[] buff, int pos) {
    return MemoryUtils.getInt(buff, pos);
  }
}
