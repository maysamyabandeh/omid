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
import java.util.ArrayList;

/**
 * The message object for the response to a commit request 
 * The response could be commit or abort
 * @author maysam
 */

public class CommitResponse implements TSOMessage {

    /**
     * Starting timestamp to uniquely identify the request
     */
    public long startTimestamp;

    /**
     * Committing timestamp
     */
    public long commitTimestamp;

    /**
     * Commited or not
     */
    public boolean committed = true;

    /**
     * Rows with write-write conflicts
     */
    public ArrayList<RowKey> rowsWithWriteWriteConflict;

    /**
     * Constructor from startTimestamp
     * @param t
     */
    public CommitResponse(long t) {
        startTimestamp = t;
    }

    public CommitResponse() {
    }

    @Override
    public void writeObject(ChannelBuffer buffer)  {
        buffer.writeLong(startTimestamp);
        buffer.writeByte((byte) (committed ? 1 : 0));
        if (committed)
            buffer.writeLong(commitTimestamp);
    }

    @Override
    public String toString() {
        return "CommitResponse: T_s:" + startTimestamp
            + " T_c:" + commitTimestamp;
    }

    @Override
    public void readObject(ChannelBuffer aInputStream) throws IOException {
        long l = aInputStream.readLong();
        startTimestamp = l;
        committed = aInputStream.readByte() == 1 ? true : false;
        if (committed) {
            l = aInputStream.readLong();
            commitTimestamp = l;
        }
        //deserialize the rows with write-write conflict
        int size = aInputStream.readInt();
        if (size != 0) {
            rowsWithWriteWriteConflict = new ArrayList<RowKey>(size);
            for (int i = 0; i < size; i++)
                rowsWithWriteWriteConflict.add(RowKey.readObject(aInputStream));
        }
    }

    @Override
    public void writeObject(DataOutputStream aOutputStream) throws IOException {
        aOutputStream.writeLong(startTimestamp);
        aOutputStream.writeByte(committed ? 1 : 0);
        if (committed)
            aOutputStream.writeLong(commitTimestamp);
        //serialize the rows with ww conflict
        //some items in rowsWithWriteWriteConflict might be null, so write as much as exist
        int size = rowsWithWriteWriteConflict == null ? 0 : rowsWithWriteWriteConflict.size();
        aOutputStream.writeInt(size);
        for (int i = 0; i < size; i++)
            rowsWithWriteWriteConflict.get(i).writeObject(aOutputStream);
    }
}
