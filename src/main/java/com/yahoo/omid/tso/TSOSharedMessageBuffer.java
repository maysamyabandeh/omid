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

package com.yahoo.omid.tso;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

import com.yahoo.omid.tso.messages.TimestampResponse;
import com.yahoo.omid.tso.messages.TimestampRequest;
import com.yahoo.omid.tso.messages.CommitRequest;
import com.yahoo.omid.tso.messages.MultiCommitRequest;
import com.yahoo.omid.client.TSOClient;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.Channels;

public class TSOSharedMessageBuffer {

   private static final Log LOG = LogFactory.getLog(TSOSharedMessageBuffer.class);
   
   private TSOState state;
   
   static long _1B = 0;
   static long _2B = 0;
   static long _AB = 0;
   static long _AS = 0;
   static long _LL = 0;
   static long _Coms = 0;
   static long _Writes = 0;
   static double _Avg = 0;
   static double _Avg2 = 0;

   static long _ha = 0;
   static long _fa = 0;
   static long _li = 0;

   static long _overflows = 0;
   static long _emptyFlushes = 0;

   public TSOSharedMessageBuffer(TSOState state) {
      this.state = state;
   }

   static private final byte m0x80 = (byte) 0x80;
   static private final byte m0x3f = (byte) 0x3f;
   static private final byte m0xff = (byte) 0xff;
   
   public void writeCommit(ChannelBuffer writeBuffer, long startTimestamp, long commitTimestamp) {
      ++_Coms;
      ++_Writes;
      int readBefore = writeBuffer.readableBytes();
      long startDiff = startTimestamp - state.latestStartTimestamp;
      long commitDiff = commitTimestamp - state.latestCommitTimestamp;
      if (commitDiff == 1 && startDiff >= -32 && startDiff <= 31) {
         ++_1B;
        startDiff &= 0x3f;
        writeBuffer.writeByte((byte) startDiff);
    } else if (commitDiff == 1 && startDiff >= -8192 && startDiff <= 8191) {
       ++_2B;
          byte high = m0x80;
          high |= (startDiff >> 8) & m0x3f; 
          byte low = (byte) (startDiff & m0xff);
          writeBuffer.writeByte(high);
          writeBuffer.writeByte(low);
      } else if (commitDiff >= Byte.MIN_VALUE && commitDiff <= Byte.MAX_VALUE) {
          if (startDiff >= Byte.MIN_VALUE && startDiff <= Byte.MAX_VALUE) {
             ++_AB;
              writeBuffer.writeByte(TSOMessage.CommittedTransactionReportByteByte);
              writeBuffer.writeByte((byte) startDiff);
          } else if (startDiff >= Short.MIN_VALUE && startDiff <= Short.MAX_VALUE) {
             ++_AS;
              writeBuffer.writeByte(TSOMessage.CommittedTransactionReportShortByte);
              writeBuffer.writeShort((short) startDiff);
          } else if (startDiff >= Integer.MIN_VALUE && startDiff <= Integer.MAX_VALUE) {
             ++_LL;
              writeBuffer.writeByte(TSOMessage.CommittedTransactionReportIntegerByte);
              writeBuffer.writeInt((int) startDiff);
          } else {
              writeBuffer.writeByte(TSOMessage.CommittedTransactionReportLongByte);
              writeBuffer.writeLong((byte) startDiff);
          }
          writeBuffer.writeByte((byte) commitDiff);
      }  else if (commitDiff >= Short.MIN_VALUE && commitDiff <= Short.MAX_VALUE) {
          if (startDiff >= Byte.MIN_VALUE && startDiff <= Byte.MAX_VALUE) {
              writeBuffer.writeByte(TSOMessage.CommittedTransactionReportByteShort);
              writeBuffer.writeByte((byte) startDiff);
          } else if (startDiff >= Short.MIN_VALUE && startDiff <= Short.MAX_VALUE) {
              writeBuffer.writeByte(TSOMessage.CommittedTransactionReportShortShort);
              writeBuffer.writeShort((short) startDiff);
          } else if (startDiff >= Integer.MIN_VALUE && startDiff <= Integer.MAX_VALUE) {
              writeBuffer.writeByte(TSOMessage.CommittedTransactionReportIntegerShort);
              writeBuffer.writeInt((int) startDiff);
          } else {
              writeBuffer.writeByte(TSOMessage.CommittedTransactionReportLongShort);
              writeBuffer.writeLong((byte) startDiff);
          }
          writeBuffer.writeShort((short) commitDiff);
      } else {
          writeBuffer.writeByte(TSOMessage.CommittedTransactionReport);
          writeBuffer.writeLong(startTimestamp);
          writeBuffer.writeLong(commitTimestamp);
       }
      int written = writeBuffer.readableBytes() - readBefore;
      
      _Avg2 += (written - _Avg2) / _Writes;
      _Avg += (written - _Avg) / _Coms;
      state.latestStartTimestamp = startTimestamp;
      state.latestCommitTimestamp = commitTimestamp;
   }

   public void writeHalfAbort(ChannelBuffer writeBuffer, long startTimestamp) {
      ++_Writes;
      int readBefore = writeBuffer.readableBytes();
      long diff = startTimestamp - state.latestHalfAbortTimestamp;
      if (diff >= -16 && diff <= 15) {
         writeBuffer.writeByte((byte)((diff & 0x1f) | (0x40)));
      } else if (diff >= Byte.MIN_VALUE && diff <= Byte.MAX_VALUE) {
          writeBuffer.writeByte(TSOMessage.AbortedTransactionReportByte);
          writeBuffer.writeByte((byte)diff);
      } else {
          writeBuffer.writeByte(TSOMessage.AbortedTransactionReport);
          writeBuffer.writeLong(startTimestamp);
      }
      ++_ha;
      
      state.latestHalfAbortTimestamp = startTimestamp;
      int written = writeBuffer.readableBytes() - readBefore;
      _Avg2 += (written - _Avg2) / _Writes;
   }

   public void writeFailedElder(ChannelBuffer writeBuffer, long startTimestamp, long commitTimestamp) {
      ++_Writes;
      int readBefore = writeBuffer.readableBytes();

      writeBuffer.writeByte(TSOMessage.FailedElderReport);
      writeBuffer.writeLong(startTimestamp);
      writeBuffer.writeLong(commitTimestamp);

      int written = writeBuffer.readableBytes() - readBefore;
      _Avg2 += (written - _Avg2) / _Writes;
   }

   public void writeEldest(ChannelBuffer writeBuffer, long startTimestamp) {
      ++_Writes;
      int readBefore = writeBuffer.readableBytes();

      writeBuffer.writeByte(TSOMessage.EldestUpdate);
      writeBuffer.writeLong(startTimestamp);

      int written = writeBuffer.readableBytes() - readBefore;
      _Avg2 += (written - _Avg2) / _Writes;
   }

   public void writeReincarnatedElder(ChannelBuffer writeBuffer, long startTimestamp) {
      ++_Writes;
      int readBefore = writeBuffer.readableBytes();

      writeBuffer.writeByte(TSOMessage.ReincarnationReport);
      writeBuffer.writeLong(startTimestamp);

      int written = writeBuffer.readableBytes() - readBefore;
      _Avg2 += (written - _Avg2) / _Writes;
   }

   public void writeFullAbort(ChannelBuffer writeBuffer, long startTimestamp) {
      ++_Writes;
      int readBefore = writeBuffer.readableBytes();
      long diff = startTimestamp - state.latestFullAbortTimestamp;
      if (diff >= -16 && diff <= 15) {
         writeBuffer.writeByte((byte)((diff & 0x1f) | (0x60)));
      } else if (diff >= Byte.MIN_VALUE && diff <= Byte.MAX_VALUE) {
          writeBuffer.writeByte(TSOMessage.FullAbortReportByte);
          writeBuffer.writeByte((byte)diff);
      } else {
          writeBuffer.writeByte(TSOMessage.FullAbortReport);
          writeBuffer.writeLong(startTimestamp);
      }
      ++_fa;
      
      state.latestFullAbortTimestamp = startTimestamp;
      int written = writeBuffer.readableBytes() - readBefore;
      _Avg2 += (written - _Avg2) / _Writes;
   }
   
   public void writeLargestIncrease(ChannelBuffer writeBuffer, long largestTimestamp) {
      ++_Writes;
      ++_li;
      int readBefore = writeBuffer.readableBytes();
      writeBuffer.writeByte(TSOMessage.LargestDeletedTimestampReport);
      writeBuffer.writeLong(largestTimestamp);
      int written = writeBuffer.readableBytes() - readBefore;
      _Avg2 += (written - _Avg2) / _Writes;
   }
   
   static long _forcedflushes = 0;
   static long _flushes = 0;
   static long _flSize = 0;
   
}

