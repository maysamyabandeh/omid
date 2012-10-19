/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved. 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License. See accompanying LICENSE file.
 */

package com.yahoo.omid.tso.persistence;

import com.yahoo.omid.tso.persistence.LoggerAsyncCallback.LoggerInitCallback;
import com.yahoo.omid.tso.persistence.LoggerAsyncCallback.AddRecordCallback;
import com.yahoo.omid.tso.persistence.LoggerAsyncCallback.ReadRangeCallback;

public interface LogBackendReader {

    /**
     * Read a range of bytes from the log backend
     * Since the data might be written as separate entries into the log backend, 
     * to read complete entries, we might need to read over the range limits.
     * In this case, the callback specifies the last read index
     * 
     * @param fromIndex range start (inclusive)
     * @param toIndex range end (inclusive)
     * @param cb the callback is called after some read data is ready to deliver
     * Note that the reading could result into multiple involation of the callback
     * The last invocation covers toIndex
     * @param ctx
     */
    void readRange(long fromIndex, long toIndex, ReadRangeCallback cb);

}

