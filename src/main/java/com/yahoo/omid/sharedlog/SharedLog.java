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

package com.yahoo.omid.sharedlog;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

public class SharedLog {
    private static final Log LOG = LogFactory.getLog(Log.class);

    private static final int CAPACITY = 1024*1024;

    /**
     * If the reads are immutable, the change in the log do not update them
     */
    public static final boolean IMMUTABLE_READ = true;

    public ChannelBuffer buffer;

    public SharedLog() {
        buffer = ChannelBuffers.directBuffer(CAPACITY);
    }

    /**
     * If x is lagged behind pointer y, the content of (x+1) is re-written by y writer
     */
    public boolean isXLaggedBehindY(long followerGlobalPointer, long followedGlobalPointer) {
        long x = followerGlobalPointer;
        long y = followedGlobalPointer;
        return (x + CAPACITY) < y;//x be rewritten by y is ok because it's already followed, but x+i is not
    }

    /**
     * convert a global pointer to a pointer on the log
     */
    public int globalToLogPointer(long x) {
        return (int)(x % CAPACITY);
    }

    /**
     * convert a global range [x,y] to the range [logX, logY] on the log
     * note: it is possible that logX > logY due to wrapp around
     */
    public LogRange globalToLogRange(long x, long y) {
        int logX = globalToLogPointer(x);
        int logY = globalToLogPointer(y);
        return new LogRange(logX, logY);
    }

    /**
     * Write the input conent at the specified pointer.
     * Wrap around the log if there is no enough space
     * assume: the contents read pointer moves forward by setBytes call
     */
    public void writeAt(long globalPointer, ChannelBuffer content) 
    throws SharedLogException {
        int writeSize = content.readableBytes();
        if (writeSize > CAPACITY)
            throw new SharedLogException("The input size is too big for the log");
        int logPointer = globalToLogPointer(globalPointer);
        int leftSpace = CAPACITY - logPointer;
        int leftOver = 0;
        if (writeSize > leftSpace) {
            leftOver = writeSize - leftSpace;
            writeSize = leftSpace;
        }
        //System.out.println("write " + writeSize + " bytes at " + logPointer);
        buffer.setBytes(logPointer, content, writeSize);
        if (leftOver > 0)
            buffer.setBytes(0, content, leftOver);
    }

    static final ChannelBuffer EMPTY_CONTENT = ChannelBuffers.wrappedBuffer(new byte[0]);
    /**
     * read the specified range from the log and return a copy
     * Note: we could return slice which is more efficient under the condition
     * that we ensure that it will be verified after the final usage. Since this is not
     * always feasible, the default is to return a copy (i.e., IMMUTABLE_READ)
     */
    public ChannelBuffer read(LogRange range) {
        int x = range.getStart();
        int y = range.getEnd();
        ChannelBuffer readBuffer;
        if (x <= y) {
            int readSize = y - x + 1;
            if (IMMUTABLE_READ)
                readBuffer = buffer.copy(x, readSize);
            else
                readBuffer = buffer.slice(x, readSize);
        } else {
            int readSize = CAPACITY - x;
            ChannelBuffer slice1 = buffer.slice(x, readSize);
            readSize = y + 1;//0,1,2,...,y
            ChannelBuffer slice2 = buffer.slice(0, readSize);
            readBuffer = ChannelBuffers.wrappedBuffer(slice1, slice2);
            if (IMMUTABLE_READ)
                readBuffer = readBuffer.copy();
        }
        return readBuffer;
    }

    public class LogRange {
        int start;
        int end;
        /**
         * assume: start,end >= 0
         */
        LogRange(int start, int end) {
            this.start = start;
            this.end = end;
        }
        public int getStart() {
            return start;
        }
        public int getEnd() {
            return end;
        }
        @Override
        public String toString() {
            return " Range: start: " + start + " end: " + end;
        }
    }

}

