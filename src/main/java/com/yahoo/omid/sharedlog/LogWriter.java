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

import java.util.concurrent.atomic.AtomicLong;
import org.jboss.netty.buffer.ChannelBuffer;

/**
 * A writer to a shared log
 * Its writes also could be followed
 * This class is designed to avoid locks. The followers use the next pointer to see if 
 * their reads are still valid: see isPointerValid
 */
public class LogWriter implements FollowedPointer {
    final private SharedLog log;
    /**
     * Last index that is written
     * There is only one writer at a time
     * It is atomic, since could could be concurrently read by readers
     */
    private AtomicLong atomicGlobalPointer;
    /**
     * next pointer is updated before the actual write
     * normally it is equal to atomicGlobalPointer
     * the content between the pointer and the next pointer is invalid during the write
     */
    private AtomicLong atomicNextGlobalPointer;
    /**
     * referece to persister
     * sometime the writer needs to slow down to its persister speed, if there is any
     * so it needs to query the persister of its speed
     */
    private LogPersister persister = null;

    public LogWriter(SharedLog log) {
        this.log = log;
        atomicGlobalPointer = new AtomicLong(0);
        atomicNextGlobalPointer = new AtomicLong(0);
    }

    public void setPersister(LogPersister persister) {
        this.persister = persister;
    }

    /**
     * The append must be synchronized since there should be only one writer at a time
     */
    public synchronized void append(ChannelBuffer content) 
    throws SharedLogException {
        long globalPointer = atomicGlobalPointer.get();
        long nextGlobalPointer = globalPointer + content.readableBytes();
        boolean tooFastWriting = false;
        if (persister != null)
            tooFastWriting = persister.wouldYouBeLaggedBehind(nextGlobalPointer);
        if (tooFastWriting)
            throw new SharedLogException("Too fast writer: no space left!");
        globalPointer++;//write to the next byte
        atomicNextGlobalPointer.set(nextGlobalPointer);
        log.writeAt(globalPointer, content);
        atomicGlobalPointer.set(nextGlobalPointer);
    }

    /**
     * What is the valid range starting after x till your pointer
     * throws SharedLogLateFollowerException if there is no valid range for x
     */
    @Override
    public SharedLog.LogRange followRangeAfter(long followerGlobalPointer) 
    throws SharedLogException {
        long x = followerGlobalPointer;
        long globalPointer = atomicGlobalPointer.get();
        if (x > globalPointer)
            throw new SharedLogException("Follower pointer " + x + " advances the subject's pointer " + globalPointer);
        if (x == globalPointer)
            return null;
        return log.globalToLogRange(x+1, globalPointer);
    }

    /**
     * check if the specified index is valid
     */
    @Override
    public boolean isPointerValid(long globalX) {
        long nextGlobalPointer = atomicNextGlobalPointer.get();
        boolean isLagged = log.isXLaggedBehindY(globalX, nextGlobalPointer);
        return !isLagged;
    }
}

