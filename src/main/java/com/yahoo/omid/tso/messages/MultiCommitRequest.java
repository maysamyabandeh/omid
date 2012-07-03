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

import java.io.DataOutputStream;
import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

import com.yahoo.omid.tso.RowKey;
import com.yahoo.omid.tso.TSOMessage;

/**
 * The message object for sending a commit request to multiple TSO
 * The difference with CommitRequest is that it has a vector timestamp
 * @author maysam
 *
 */
public class MultiCommitRequest extends CommitRequest {

    /**
     * The id of the reading status oracle
     * Each status oracle reads a different Ts from the vector timestamp
     * This is to avoid the cost of creating a new CommitRequest message
     */
    int soId = -1;//not need to be serialized, set at the read time
    public void setSoId(int id) {
        soId = id;
    }

    /**
     * Starting timestamp
     */
    private long[] startTimestamps = null;
    @Override
    public long getStartTimestamp() {
        return startTimestamps[soId];
    }
    @Override
    public void setStartTimestamp(long ts) {
        startTimestamps[soId] = ts;
    }

    public MultiCommitRequest() {
        globalTxn = true;
    }

    public MultiCommitRequest(long[] startTimestamps) {
        this();
        this.startTimestamps = startTimestamps;
        this.writtenRows = new RowKey[0];
        this.readRows = new RowKey[0];
    }

    public MultiCommitRequest(long[] startTimestamps, RowKey[] writtenRows) {
        this();
        this.startTimestamps = startTimestamps;
        this.writtenRows = writtenRows;
        this.readRows = new RowKey[0];
    }

    public MultiCommitRequest(long[] startTimestamps, RowKey[] writtenRows, RowKey[] readRows) {
        this();
        this.startTimestamps = startTimestamps;
        this.writtenRows = writtenRows;
        this.readRows = readRows;
    }

    @Override
    public String toString() {
        return "MultiCommitRequest: T_s:" + java.util.Arrays.toString(startTimestamps) + " prepared: " + prepared;
    }


    @Override
    public void readObject(ChannelBuffer aInputStream) throws IOException {
        super.readObject(aInputStream);
        int size = aInputStream.readInt();
        startTimestamps = new long[size];
        for (int i = 0; i < size; i++)
            startTimestamps[i] = aInputStream.readLong();
    }

    @Override
    public void writeObject(ChannelBuffer buffer)  {
        super.writeObject(buffer);
        buffer.writeInt(startTimestamps.length);
        for (int i = 0; i < startTimestamps.length; i++)
            buffer.writeLong(startTimestamps[i]);
    }

    @Override
    public void writeObject(DataOutputStream aOutputStream) throws IOException {
        super.writeObject(aOutputStream);
        aOutputStream.writeInt(startTimestamps.length);
        for (int i = 0; i < startTimestamps.length; i++)
            aOutputStream.writeLong(startTimestamps[i]);
    }
}


