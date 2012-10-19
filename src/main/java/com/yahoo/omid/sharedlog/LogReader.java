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

import com.yahoo.omid.Statistics;

import java.util.concurrent.atomic.AtomicLong;
import org.jboss.netty.buffer.ChannelBuffer;

/**
 * A reader from a shared log
 * It follows the writes of a subject
 */
public class LogReader {
    private SharedLog log;
    /**
     * last read index
     */
    private long globalPointer;
    /**
     * the last read data is in the range between lastGlobalPointer and globalPointer
     */
    private long lastGlobalPointer;
    private FollowedPointer subject;

    public LogReader(SharedLog log, FollowedPointer subject, long startingGlobalIndex) {
        this.log = log;
        this.subject = subject;
        this.globalPointer = startingGlobalIndex;
        this.lastGlobalPointer = this.globalPointer;
    }

    /**
     * read the newly generated content by the subject
     * return null if there is no new content
     * the return content is immutable, it is enough to be verified after read
     * otherwise should be verified after final usage
     */
    public ChannelBuffer tail() 
    throws SharedLogException, SharedLogLateFollowerException {
        SharedLog.LogRange range = subject.followRangeAfter(globalPointer);
        if (range == null) {
            Statistics.fullReport(Statistics.Tag.EMPTY_FOR_BROADCAST, 1);
            return null;
        }
        //System.out.println(range);
        ChannelBuffer buffer = log.read(range);
        lastGlobalPointer = globalPointer;
        globalPointer += buffer.readableBytes();
        //concurrency check: since we do not use locks, we should check the read content
        //was not being modified concurrently
        if (SharedLog.IMMUTABLE_READ) //then do the check now
            verifyLastRead();
        //otherwise, the verification should be called when the user eventually finishes consuming the read data
        return buffer;
    }

    /**
     * Verify if the last read is not overwritten by a concurrent write (or subsequent write in the case the read is not immutable) 
     */
    public void verifyLastRead() 
    throws SharedLogLateFollowerException {
        boolean isValid = subject.isPointerValid(lastGlobalPointer);
        if (!isValid)
            throw new SharedLogLateFollowerException();
    }

    @Override
    public String toString() {
        return "LogReader: globalPointer: " + globalPointer + " lastGlobalPointer: " + lastGlobalPointer;
    }
}


