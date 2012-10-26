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

package com.yahoo.omid.tso.serialization;

import java.io.EOFException;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.io.HalfStoreFileReader;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

import com.yahoo.omid.tso.TSOMessage;
import com.yahoo.omid.tso.messages.AbortRequest;
import com.yahoo.omid.tso.messages.AbortedTransactionReport;
import com.yahoo.omid.tso.messages.CommitQueryRequest;
import com.yahoo.omid.tso.messages.CommitQueryResponse;
import com.yahoo.omid.tso.messages.CommitRequest;
import com.yahoo.omid.tso.messages.CommitResponse;
import com.yahoo.omid.tso.messages.PrepareCommit;
import com.yahoo.omid.tso.messages.PrepareResponse;
import com.yahoo.omid.tso.messages.CommittedTransactionReport;
import com.yahoo.omid.tso.messages.FullAbortReport;
import com.yahoo.omid.tso.messages.FailedElderReport;
import com.yahoo.omid.tso.messages.EldestUpdate;
import com.yahoo.omid.tso.messages.ReincarnationReport;
import com.yahoo.omid.tso.messages.LargestDeletedTimestampReport;
import com.yahoo.omid.tso.messages.TimestampRequest;
import com.yahoo.omid.tso.messages.TimestampResponse;
import com.yahoo.omid.tso.messages.MultiCommitRequest;

import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicReference;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ChannelUpstreamHandler;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.replay.ReplayingDecoder;

import com.yahoo.omid.tso.SequencerHandler;

/**
 * This decoder is used by the sequencer, the node that broadcast the recieved messages.
 * It is designed to avoid decoding of the message content, and simply detect only 
 * the boundries
 */
public class SeqDecoder extends FrameDecoder {
    private static final Log LOG = LogFactory.getLog(SeqDecoder.class);

    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buf) throws Exception {
        int readerIndex = buf.readerIndex();
WHILE:
        while (buf.readable()) {
            // Mark the current buffer position before any reading
            // because the whole frame might not be in the buffer yet.
            // We will reset the buffer position to the marked position if
            // there's not enough bytes in the buffer.
            buf.markReaderIndex();

            try {
                byte type = buf.readByte();
                if (LOG.isTraceEnabled())
                    LOG.trace("Decoding message : " + type);
                switch (type) {
                    case TSOMessage.TimestampRequest:
                    case TSOMessage.MultiCommitRequest:
                        int size = buf.readShort()-1-2;//already has read type and size
                        if (size > buf.readableBytes()) {//avoid exception
                            buf.resetReaderIndex();
                            break WHILE;
                        }
                        buf.skipBytes(size);
                        break;
                    default:
                        throw new Exception("Wrong type " + type + " " + buf.toString().length());
                }
            } catch (IndexOutOfBoundsException e) {
                // Not enough byte in the buffer, reset to the start for the next try
                buf.resetReaderIndex();
                break WHILE;
            } catch (EOFException e) {
                // Not enough byte in the buffer, reset to the start for the next try
                buf.resetReaderIndex();
                break WHILE;
            }
        }

        int currReaderIndex = buf.readerIndex();
        int readLength = currReaderIndex - readerIndex;
        if (readLength == 0)
            return null;
        ChannelBuffer slice = buf.copy(readerIndex, readLength);
        //ChannelBuffer slice = buf.slice(readerIndex, readLength);
        //Note: You could use slice if you directly consume the returned slice. If the slice is otherwise put in a buffer, you should get a fresh copy of the slice
        return slice;
    }
}


