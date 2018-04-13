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

import org.apache.drill.exec.memory.BaseAllocator;
import org.apache.drill.jmh.common.utils.MemoryUtils;

import io.netty.buffer.DrillBuf;

/** Emulate a variable length value vector */
public class VarCharValueVector {
  public final DrillBuf offsets;
  public final DrillBuf data;
  public int numValues;
  private BufferedMutator mutator;

  public VarCharValueVector(int maxNumValues, int dataCapacity, BaseAllocator allocator) {
    offsets = allocator.buffer(4 * (maxNumValues + 1));
    data    = allocator.buffer(dataCapacity);
    mutator = new BufferedMutator(this);
  }

  public void setValueCapacity(int numValues) {
    this.numValues = numValues;
  }

  public void reset() {
    numValues = 0;
    mutator.reset();
  }

  public void close() {
    offsets.release();
    data.release();
  }

  public void setSafe(BulkEntry bulkEntry) {
    mutator.setSafe(bulkEntry);
  }

  public int getEntryLen(int index) {
    if (index >= numValues) {
      throw new IndexOutOfBoundsException();
    }
    int start = offsets.getInt(index * 4);
    int end   = offsets.getInt((index + 1) * 4);

    return end - start;
  }

  public String getEntryData(int index) {
    if (index >= numValues) {
      throw new IndexOutOfBoundsException();
    }
    int start = offsets.getInt(index * 4);
    int end   = offsets.getInt((index + 1) * 4);

    byte[] dest = new byte[end - start];
    data.getBytes(start, dest);

    return new String(dest);
  }

// ----------------------------------------------------------------------------
// Inner Data Structure
// ----------------------------------------------------------------------------

  /**
   * Helper class to buffer container mutation as a means to optimize native memory copy operations. Ideally, this
   * should be done transparently as part of the Mutator and Accessor APIs.
   *
   * NB: this class is automatically generated from ValueVectorTypes.tdd using FreeMarker.
   */
  private static final class BufferedMutator {
    /** The default buffer size */
    private static final int DEFAULT_BUFF_SZ = 1 << 12;
    /** Data Byte buffer */
    private final ByteBuffer buffer;
    /** Offsets Byte buffer */
    private final ByteBuffer offsetsBuffer;

    /** Current offset within the data buffer */
    private int dataBuffOff;
    /** Total data length (contained within data and buffer) */
    private int totalDataLen;
    /** Offsets index */
    private int offsetsBuffOff;
    /** Parent object */
    private final VarCharValueVector parent;
    /** Indicator on whether to enable data buffering; this might not be useful
     *  as the bulk entry has it buffered
     */
    private final boolean enableDataBuffering;

    public BufferedMutator(VarCharValueVector parent) {
      this(DEFAULT_BUFF_SZ, parent);
    }

    /**
     * Buffered mutator to optimize bulk access to the underlying vector container
     * @param _buff_sz buffer length to us
     * @param _parent parent container object
     */
    private BufferedMutator(int _buff_sz, VarCharValueVector parent) {
      this.parent              = parent;
      this.enableDataBuffering = false;
      this.offsetsBuffer       = ByteBuffer.allocate(_buff_sz);
      offsetsBuffer.order(ByteOrder.nativeOrder());

      if (enableDataBuffering) {
        this.buffer = ByteBuffer.allocate(_buff_sz);
        buffer.order(ByteOrder.nativeOrder());
      } else {
        buffer = null;
      }

      this.dataBuffOff    = 0;
      this.totalDataLen   = dataBuffOff;
      this.offsetsBuffOff = 1;
    }

    private void setSafe(BulkEntry bulkEntry) {
      assert bulkEntry.arrayBacked();

      // The new entry doesn't fit in remaining space
      if (enableDataBuffering && buffer.remaining() < bulkEntry.getTotalLength()) {
        flushData();
      }

      // We need to transform the source length array into an offset array; will reuse the input array
      // to minimize allocations.
      int[] offsets  = bulkEntry.getValuesLength();
      int num_values = bulkEntry.getNumValues();

      // Now update the offsets vector with new information
      setOffsets(offsets, num_values);

      // Now we're able to buffer the new bulk entry
      if (enableDataBuffering && buffer.remaining() >= bulkEntry.getTotalLength()) {
        buffer.put(bulkEntry.getArrayData(), bulkEntry.getDataStartOffset(), bulkEntry.getTotalLength());

      } else {
        parent.data.setBytes(dataBuffOff, bulkEntry.getArrayData(), bulkEntry.getDataStartOffset(), bulkEntry.getTotalLength());
        dataBuffOff += bulkEntry.getTotalLength();
      }
    }

    private void reset() {
      flushOffsets();
      flushData();

      dataBuffOff    = 0;
      totalDataLen   = dataBuffOff;
      offsetsBuffOff = 4; // Start at index 1 (which translates into byte offset 4)
    }

    private void setOffsets(int[] lengths, int num_values) {
      // We need to compute source offsets using the current target offset and the value length array.
      final byte[] buffer_array = offsetsBuffer.array();
      int remaining             = num_values;
      int srcPos                = 0;

      do {
        if (offsetsBuffer.remaining() < 4) {
          flushOffsets();
        }

        final int toCopy = Math.min(remaining, offsetsBuffer.remaining() / 4);
        int tgtPos       = offsetsBuffer.position();

        for (int idx = 0; idx < toCopy; idx++, tgtPos += 4, srcPos++) {
          totalDataLen += lengths[srcPos];
          MemoryUtils.putInt(buffer_array, tgtPos, totalDataLen);
        }

        // Update counters
        offsetsBuffer.position(tgtPos);
        remaining -= toCopy;

      } while (remaining > 0);
    }

    private void flushData() {
      if (!enableDataBuffering) {
        return; // NOOP
      }

      int numBytes = buffer.position();

      if (numBytes == 0) {
        return; // NOOP
      }

      parent.data.setBytes(dataBuffOff, buffer.array(), 0, numBytes);
      dataBuffOff += numBytes;

      // Reset the byte buffer
      buffer.clear();
    }

    private void flushOffsets() {
      int numBytes = offsetsBuffer.position();

      if (numBytes == 0) {
        return; // NOOP
      }

      parent.offsets.setBytes(offsetsBuffOff, offsetsBuffer.array(), 0, numBytes);
      offsetsBuffOff += numBytes;

      // Reset the byte buffer
      offsetsBuffer.clear();
    }
  }

}
