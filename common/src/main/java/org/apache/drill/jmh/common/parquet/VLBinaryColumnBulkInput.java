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

/** Implements the {@link VarBinaryVector.BulkInput} interface to optimize data copy */
public final class VLBinaryColumnBulkInput implements BulkInput {
  /** The records to read */
  private int recordsToRead;
  /** Bulk state object */
  private final BulkReadState readState;
  /** Buffered page payload */
  private VLBulkPageReader bulkPayloadReader;
  /** Page Reader */
  final PageReader pageReader;
  /** Expected data length */
  private final int expectedDataLen;

  VLBinaryColumnBulkInput(PageReader _pageReader) {
    this.pageReader      = _pageReader;
    this.readState       = new BulkReadState(pageReader.pageReadyToReadPosInBytes, pageReader.pageValuesRead, 0);
    this.expectedDataLen = pageReader.expDataSize;

    // Initialize the buffered page payload object if there are page(s) to be processed
    if (pageReader.hasPage() && readState.numPageFieldsRead < pageReader.currentPageCount) {
      bulkPayloadReader = new VLBulkPageReader(pageReader.isFixedSize(),
        pageReader.pageData, (int) readState.pageReadPos, pageReader.pageByteCount, expectedDataLen);
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean hasNext() {
    try {
      if (readState.batchFieldIndex < recordsToRead) {
        // We need to ensure there is a page of data to be read
        if (!pageReader.hasPage() || pageReader.currentPageCount == readState.numPageFieldsRead) {
          long totalValueCount = pageReader.totalValuesToRead;

          if (totalValueCount == (pageReader.totalValuesRead + readState.batchFieldIndex) || !pageReader.next()) {
            return false;
          }

          // Reset the state object page read metadata
          readState.numPageFieldsRead = 0;
          readState.pageReadPos       = pageReader.pageReadyToReadPosInBytes;

          // Create a new instance of the buffered page payload since we've read a new page
          if (bulkPayloadReader == null) {
            bulkPayloadReader = new VLBulkPageReader(pageReader.isFixedSize(),
              pageReader.pageData, (int) readState.pageReadPos, pageReader.pageByteCount, expectedDataLen);
          } else {
            bulkPayloadReader.set(
              pageReader.pageData, (int) readState.pageReadPos, pageReader.pageByteCount);
          }
        }
        return true;
      } else {
        return false;
      }
    } catch (Exception ie) {
      throw new RuntimeException(ie);
    }
  }

  /** {@inheritDoc} */
  @Override
  public BulkEntry next() {
    int remaining    = recordsToRead - readState.batchFieldIndex;
    BulkEntry result = bulkPayloadReader.getEntry(remaining);

    if (result != null) {
      // Update position for next read
      readState.pageReadPos       += (result.getTotalLength() + 4 * result.getNumValues());
      readState.numPageFieldsRead += result.getNumValues();
      readState.batchFieldIndex   += result.getNumValues();
    } else {
      throw new RuntimeException("result is null");
    }
    return result;
  }

  /** {@inheritDoc} */
  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  /** {@inheritDoc} */
  @Override
  public int getStartIndex() {
    return readState.batchFieldIndex;
  }

  /** {@inheritDoc} */
  @Override
  public void done() {
    // Update the page reader state so that a future call to this method resumes
    // where we left off.
    pageReader.pageValuesRead            = readState.numPageFieldsRead;
    pageReader.pageReadyToReadPosInBytes = readState.pageReadPos;
    pageReader.totalValuesRead          += readState.batchFieldIndex;
  }

  public void newBatch(int recordsToRead) {
    this.recordsToRead          = recordsToRead;
    readState.pageReadPos       = pageReader.pageReadyToReadPosInBytes;
    readState.numPageFieldsRead = pageReader.pageValuesRead;
    readState.batchFieldIndex   = 0;
  }

// ----------------------------------------------------------------------------
// Inner Classes
// ----------------------------------------------------------------------------

  /**
   * Contains information about current bulk read operation
   */
  public static final class BulkReadState {
    /** reader position within current page */
    private int pageReadPos;
    /** number of fields read within current page */
    private int numPageFieldsRead;
    /** field index within current batch */
    private int batchFieldIndex;

      private BulkReadState(int pageReadPos, int numPageFieldsRead, int batchFieldIndex) {
        this.pageReadPos       = pageReadPos;
        this.numPageFieldsRead = numPageFieldsRead;
        this.batchFieldIndex   = batchFieldIndex;
      }
  }


}