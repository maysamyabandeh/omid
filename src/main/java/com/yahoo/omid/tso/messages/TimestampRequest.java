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
 * The message object for sending a timestamp request to TSO
 * @author maysam
 *
 */
public class TimestampRequest implements TSOMessage {
    /**
     * should we track the progress of the timestamp that is assigned to this txn.
     * or it is just a sequence request
     */
    public boolean trackProgress = true;

    /**
     * is this request sequenced and if yes what is the sequence number
     * -1 means no sequence
     */
    public long sequence = -1;

    public boolean isSequenced() {
        return sequence != -1;
    }

	@Override
   public void writeObject(DataOutputStream aOutputStream) 
      throws IOException {
      aOutputStream.writeByte(trackProgress ? 1 : 0);
      if (isSequenced()) {
          aOutputStream.writeByte(1);
          aOutputStream.writeLong(sequence);
      } else {
          aOutputStream.writeByte(0);
      }
   }

	@Override
	public void readObject(ChannelBuffer aInputStream) throws IOException {
       byte b = aInputStream.readByte();
       trackProgress = b == 1 ? true : false;
       byte s = aInputStream.readByte();
       if (s == 1) { //isSequenced
           sequence = aInputStream.readLong();
       }
	}

	   @Override
      public void writeObject(ChannelBuffer buffer)  {
          buffer.writeByte(trackProgress ? 1 : 0);
          if (isSequenced()) {
              buffer.writeByte(1);
              buffer.writeLong(sequence);
          } else {
              buffer.writeByte(0);
          }
      }
}


