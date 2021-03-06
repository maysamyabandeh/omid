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

package com.yahoo.omid.tso.messages;

import java.io.*;
import java.nio.ByteBuffer;

import org.jboss.netty.buffer.ChannelBuffer;

import com.yahoo.omid.tso.TSOMessage;

/**
 * The message object for reporting the finish of reincarnation
 * @author maysam
 *
 */
public class ReincarnationReport implements TSOMessage {

    /**
     * the start timestamp of the reincarnated transaction
     */
    public long startTimestamp;

    /**
     * Constructor from timestamp
     */
    public ReincarnationReport() {
    }

    /**
     * Constructor from timestamp
     * @param t
     */
    public ReincarnationReport(long t) {
        startTimestamp = t;
    }

    @Override
    public String toString() {
        return "ReincarnationReport: T_s:" + startTimestamp;
    }

    @Override
    public void readObject(ChannelBuffer aInputStream) 
    throws IOException {
    startTimestamp = aInputStream.readLong();
    }

    @Override
    public void writeObject(DataOutputStream aOutputStream) 
    throws IOException {
    aOutputStream.writeLong(startTimestamp);
    }


    @Override
    public void writeObject(ChannelBuffer buffer)
    {
        buffer.writeLong(startTimestamp);
    }
}


