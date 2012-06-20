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

import com.yahoo.omid.tso.TSOMessage;

/**
 * The message object for sending a commit request to TSO
 * 
 * @author maysam
 * 
 */
public class TimestampResponse implements TSOMessage, Sequencable {

   /**
    * the timestamp
    */
   public long timestamp;

   public boolean isFailed() {
       return timestamp == -1;
   }

    public static TimestampResponse failedResponse(long seq) {
        TimestampResponse tr = new TimestampResponse();
        tr.timestamp = -1;//means failed
        tr.sequence = seq;
        return tr;
    }

    /**
     * is this request sequenced and if yes what is the sequence number
     * -1 means no sequence
     */
    public long sequence = -1;

    public long getSequence() {
        return sequence;
    }

    public boolean isSequenced() {
        return sequence != -1;
    }

   /**
    * Constructor from timestamp
    * 
    * @param t
    */
   public TimestampResponse(long t) {
      timestamp = t;
   }

   public TimestampResponse(long t, long seq) {
      timestamp = t;
      sequence = seq;
   }

   public TimestampResponse() {
   }

   @Override
   public String toString() {
      return "TimestampResponse: T_s:" + timestamp;
   }

   /**
    * hack: update this function whenever you change writeObject function
    */
   public static int sizeInBytes() {
       return 17;//byte + long + long
   }

   @Override
   public void writeObject(ChannelBuffer buffer) {
      buffer.writeLong(timestamp);
      if (isSequenced()) {
          buffer.writeByte(1);
          buffer.writeLong(sequence);
      } else {
          buffer.writeByte(0);
      }
       //NOTE: update sizeInBytes as well
   }

   @Override
   public void readObject(ChannelBuffer aInputStream) throws IOException {
      long l = aInputStream.readLong();
      timestamp = l;
      byte s = aInputStream.readByte();
      if (s == 1) { //isSequenced
          sequence = aInputStream.readLong();
      }
   }

//   static byte[] dummy = new byte [1100];
   @Override
   public void writeObject(DataOutputStream aOutputStream) throws IOException {
      aOutputStream.writeLong(timestamp);
      if (isSequenced()) {
          aOutputStream.writeByte(1);
          aOutputStream.writeLong(sequence);
      } else {
          aOutputStream.writeByte(0);
      }
   }
}
