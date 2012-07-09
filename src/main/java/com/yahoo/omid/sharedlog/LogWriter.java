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
 */
public class LogWriter implements FollowedPointer {
    private SharedLog log;
    /**
     * Last index that is written
     */
    private AtomicLong atomicGlobalPointer;
    /**
     * next pointer is updated before the actual write
     * normally it is equal to atomicGlobalPointer
     * the content between the pointer and the next pointer is invalid during the write
     */
    private AtomicLong atomicNextGlobalPointer;

    public LogWriter(SharedLog log) {
        this.log = log;
        atomicGlobalPointer = new AtomicLong(0);
        atomicNextGlobalPointer = new AtomicLong(0);
    }

    /**
     * The append must be synchronized since there should be only one writer at a time
     */
    public synchronized void append(ChannelBuffer content) 
    throws SharedLogException {
        long globalPointer = atomicGlobalPointer.get();
        long nextGlobalPointer = globalPointer + content.readableBytes();
        globalPointer++;//write to the next byte
        //System.out.println("append: globalPointer is " + globalPointer);
        //System.out.println("append: nextGlobalPointer is " + globalPointer);
        atomicNextGlobalPointer.set(nextGlobalPointer);
        log.writeAt(globalPointer, content);
        atomicGlobalPointer.set(nextGlobalPointer);
        //System.out.println("append: atomicGlobalPointer is " + atomicGlobalPointer + " but it should be " + nextGlobalPointer);
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
        if (log.isXLaggedBehindY(x, globalPointer))
            throw new SharedLogLateFollowerException();
        return log.globalToLogRange(x+1, globalPointer);
    }

    /**
     * check if the specified range is valid
     */
    @Override
    public boolean isPointerValid(long globalX) {
        long nextGlobalPointer = atomicNextGlobalPointer.get();
        boolean isLagged = log.isXLaggedBehindY(globalX, nextGlobalPointer);
        return !isLagged;
    }
}

