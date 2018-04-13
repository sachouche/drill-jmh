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

import java.util.Iterator;

/**
 * Allows caller to provide input in a bulk manner while abstracting the underlying data structure
 * to provide performance optimizations opportunities.
 */
public interface BulkInput extends Iterator<BulkEntry> {
  /**
   * @return start index of this bulk input (relative to this VL container)
   */
  int getStartIndex();
  /**
   * Indicates we're done processing (processor might stop processing when memory buffers
   * are depleted); this allows caller to re-submit any unprocessed data.
   *
   * @param numCommitted number of processed entries
   */
  void done();
}