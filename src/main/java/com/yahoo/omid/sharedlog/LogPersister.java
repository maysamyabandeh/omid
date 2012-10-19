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
 * Note: NOT THREAD-SAFE: only one thread should work with this object
 *
 * A persister follows after a writer and is followed by some readers
 * LogPersister could be followed by multiple threads but must be used for persistence
 * only by one thread. Also, only one on-the-flight to-be-persisted is allowed. In other
 * words, toBePersisted must be called only after the successful persistence 
 * of the piece returned by the last toBePersisted call.
 *
 * This class is designed to avoid locks. The followers use the persisting 
 * pointer to see if their reads are still valid: see isPointerValid
 */
public class LogPersister implements FollowedPointer {
    private SharedLog log;

    /**
     * The subject that is persisted by this LogPersister
     */
    private FollowedPointer subject;

    /**
     * Last index that is persisted
     */
    private AtomicLong atomicPersistedGlobalPointer;

    /**
     * the pointer to the last byte sent for persistence
     * the content between the persisted and persisting is not safely persisted yet
     */
    private AtomicLong atomicPersistingGlobalPointer;

    public LogPersister(SharedLog log, FollowedPointer subject) {
        this.log = log;
        this.subject = subject;
        atomicPersistedGlobalPointer = new AtomicLong(0);
        atomicPersistingGlobalPointer = new AtomicLong(0);
    }

    public final long getGlobalPointer() {
        return atomicPersistedGlobalPointer.get();
    }

    /**
     * read the newly generated content by the subject
     * return null if there is no new content
     * The content need not to be verified since other mechanisms must ensure that the
     * data is not erased before persistence
     * Note: after persistence ack is recieved, the persisted() should be invoked
     * to move the pointer forward
     * Note: NOT THREAD-SAFE: only one thread to be invoking this method
     */
    public ToBePersistedData toBePersisted()
    throws SharedLogException, SharedLogLateFollowerException {
        long persistingPointer = atomicPersistingGlobalPointer.get();
        SharedLog.LogRange range = subject.followRangeAfter(persistingPointer);
        if (range == null) {
            Statistics.fullReport(Statistics.Tag.EMPTY_FOR_PERSISTENCE, 1);
            return null;
        }
        ChannelBuffer buffer = log.read(range);
        Statistics.fullReport(Statistics.Tag.TO_BE_PERSISTED_SIZE, buffer.readableBytes());
        persistingPointer += buffer.readableBytes();
        atomicPersistingGlobalPointer.set(persistingPointer);
        //Note: normally the follower should do a concurrency check to ensure that the 
        //read data is not concurrently modified by the subject, however, here since
        //the writer waits for the persister to finish its job (using 
        //wouldYouBeLaggedBehind), there is no need for this check here
        return new ToBePersistedData(buffer, persistingPointer);
    }

    /**
     * The callback to be invoked after the data is persisted
     */
    public class ToBePersistedData {
        long persistingGlobalPointer;
        ChannelBuffer data;
        ToBePersistedData(ChannelBuffer data, long pointer) {
            persistingGlobalPointer = pointer;
            this.data = data;
        }
        public ChannelBuffer getData() {
            return data;
        }
        public void persisted() {
            LogPersister.this.persisted(persistingGlobalPointer);
        }
    }

    /**
     * Confirm the persitence
     * must be called after receiving the persistence ack
     * must be synchronized since multiple acks could cause concurrent invocations
     * @assume: persistance ack for x implies successful persistence of all y < x
     */
    synchronized void persisted(long persistedGlobalPointer) {
        long current = atomicPersistedGlobalPointer.get();
        if (persistedGlobalPointer > current)
            atomicPersistedGlobalPointer.set(persistedGlobalPointer);
    }

    /**
     * A writer could you this method to see if its write would erase the data
     * that is not persisted yet
     */
    public boolean wouldYouBeLaggedBehind(long targetGlobalPointer) {
        long committedGlobalPointer = atomicPersistedGlobalPointer.get();
        boolean answer = log.isXLaggedBehindY(committedGlobalPointer, targetGlobalPointer);
        return answer;
    }


    /**
     * What is the valid range starting after x till your pointer
     * throws SharedLogLateFollowerException if there is no valid range for x
     */
    @Override
    public SharedLog.LogRange followRangeAfter(long followerGlobalPointer) 
    throws SharedLogException {
        long x = followerGlobalPointer;
        long globalPointer = atomicPersistedGlobalPointer.get();
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
        //a global pointer is valid if it is not overwritten by the writer (our subject)
        return subject.isPointerValid(globalX);
    }
}




