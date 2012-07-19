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
 * The message object for annoncing the id to the peer
 * @author maysam
 *
 */
public class PeerIdAnnoncement extends SizedTSOMessage implements TSOMessage {
    /**
     * what is the peer id
     */
    public int peerId;

    public PeerIdAnnoncement() {
    }

    public PeerIdAnnoncement(int peerId) {
        this.peerId = peerId;
    }

    public int getPeerId() {
        return peerId;
    }

    @Override
    public String toString() {
        return "PeerIdAnnoncement " + peerId;
    }

    @Override
    public void writeObject(DataOutputStream aOutputStream) throws IOException {
        aOutputStream.writeInt(peerId);
    }

    @Override
    public void readObject(ChannelBuffer aInputStream) throws IOException {
        peerId = aInputStream.readInt();
        //TODO: improvisation to make it work with fake PeerIdAnnoncement
        //It should be removed later
        setSize(1 /*type*/ + 4 /*peerId*/);
    }

    @Override
    public void writeObject(ChannelBuffer buffer)  {
        buffer.writeInt(peerId);
    }
}



