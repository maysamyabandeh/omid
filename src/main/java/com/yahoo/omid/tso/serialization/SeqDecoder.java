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
//import org.jboss.netty.handler.codec.frame.FrameDecoder;

import com.yahoo.omid.tso.TSOMessage;
import com.yahoo.omid.tso.TSOSharedMessageBuffer;
import com.yahoo.omid.tso.messages.AbortRequest;
import com.yahoo.omid.tso.messages.PeerIdAnnoncement;
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
import com.yahoo.omid.tso.messages.PeerIdAnnoncement;
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

public class SeqDecoder extends FrameDecoder {
    private static final Log LOG = LogFactory.getLog(SeqDecoder.class);

    public SeqDecoder(SequencerHandler sh) {
        super(sh);
    }

    @Override
    protected void messageReceived(
            ChannelHandlerContext ctx, MessageEvent e, ChannelBuffer cumulation, ChannelBuffer input) throws Exception {
        if (cumulation.readable()) {
            cumulation.discardReadBytes();
            cumulation.writeBytes(input);
            //cumulation = ChannelBuffers.wrappedBuffer(cumulation, input);
            callDecode(ctx, e.getChannel(), cumulation, e.getRemoteAddress());
            //this.cumulation = input;
        } else {
            callDecode(ctx, e.getChannel(), input, e.getRemoteAddress());
            if (input.readable()) {
                cumulation.writeBytes(input);
            }
        }
    }

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
                        int size = buf.readShort()-1-2;//alread read type and size
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
        //System.out.println("len: " + readLength + " cri: " + currReaderIndex +
                //" ri: " + readerIndex + " buf.ri: " + buf.readerIndex() + " msg: " + msg);
        return slice;
    }
}

@ChannelPipelineCoverage("one")
abstract class FrameDecoder extends SimpleChannelUpstreamHandler {

    private final boolean unfold;
    protected SequencerHandler sequencerHandler;
    protected ChannelBuffer cumulation = null;
    //private final AtomicReference<ChannelBuffer> cumulation =
        //new AtomicReference<ChannelBuffer>();

    protected FrameDecoder(SequencerHandler sh) {
        this(false);
        sequencerHandler = sh;
    }

    protected FrameDecoder(boolean unfold) {
        this.unfold = unfold;
    }

    @Override
    public void messageReceived(
            ChannelHandlerContext ctx, MessageEvent e) throws Exception {

        Object m = e.getMessage();
        if (!(m instanceof ChannelBuffer)) {
            ctx.sendUpstream(e);
            return;
        }

        ChannelBuffer input = (ChannelBuffer) m;
        if (!input.readable()) {
            return;
        }

        ChannelBuffer cumulation = cumulation(ctx);
        messageReceived(ctx, e, cumulation, input);
    }

    protected void messageReceived(
            ChannelHandlerContext ctx, MessageEvent e, ChannelBuffer cumulation, ChannelBuffer input) throws Exception {
        if (cumulation.readable()) {
            cumulation.discardReadBytes();
            cumulation.writeBytes(input);
            callDecode(ctx, e.getChannel(), cumulation, e.getRemoteAddress());
        } else {
            callDecode(ctx, e.getChannel(), input, e.getRemoteAddress());
            if (input.readable()) {
                cumulation.writeBytes(input);
            }
        }
    }

    @Override
    public void channelDisconnected(
            ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        cleanup(ctx, e);
    }

    @Override
    public void channelClosed(
            ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        cleanup(ctx, e);
    }

    @Override
    public void exceptionCaught(
            ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        ctx.sendUpstream(e);
    }

    /**
     * Decodes the received packets so far into a frame.
     *
     * @param ctx      the context of this handler
     * @param channel  the current channel
     * @param buffer   the cumulative buffer of received packets so far.
     *                 Note that the buffer might be empty, which means you
     *                 should not make an assumption that the buffer contains
     *                 at least one byte in your decoder implementation.
     *
     * @return the decoded frame if a full frame was received and decoded.
     *         {@code null} if there's not enough data in the buffer to decode a frame.
     */
    protected abstract Object decode(
            ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception;

    /**
     * Decodes the received data so far into a frame when the channel is
     * disconnected.
     *
     * @param ctx      the context of this handler
     * @param channel  the current channel
     * @param buffer   the cumulative buffer of received packets so far.
     *                 Note that the buffer might be empty, which means you
     *                 should not make an assumption that the buffer contains
     *                 at least one byte in your decoder implementation.
     *
     * @return the decoded frame if a full frame was received and decoded.
     *         {@code null} if there's not enough data in the buffer to decode a frame.
     */
    protected Object decodeLast(
            ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception {
        return decode(ctx, channel, buffer);
    }

    protected void callDecode(
            ChannelHandlerContext context, Channel channel,
            ChannelBuffer cumulation, SocketAddress remoteAddress) throws Exception {

        while (cumulation.readable()) {
            int oldReaderIndex = cumulation.readerIndex();
            Object frame = decode(context, channel, cumulation);
            if (frame == null) {
                if (oldReaderIndex == cumulation.readerIndex()) {
                    // Seems like more data is required.
                    // Let us wait for the next notification.
                    break;
                } else {
                    // Previous data has been discarded.
                    // Probably it is reading on.
                    continue;
                }
            } else if (oldReaderIndex == cumulation.readerIndex()) {
                throw new IllegalStateException(
                        "decode() method must read at least one byte " +
                        "if it returned a frame (caused by: " + getClass() + ")");
            }

            unfoldAndFireMessageReceived(context, remoteAddress, frame);
        }
    }

    private void unfoldAndFireMessageReceived(ChannelHandlerContext context, SocketAddress remoteAddress, Object result) {
        if (unfold) {
            if (result instanceof Object[]) {
                for (Object r: (Object[]) result) {
                    Channels.fireMessageReceived(context, r, remoteAddress);
                }
            } else if (result instanceof Iterable<?>) {
                for (Object r: (Iterable<?>) result) {
                    Channels.fireMessageReceived(context, r, remoteAddress);
                }
            } else {
                Channels.fireMessageReceived(context, result, remoteAddress);
            }
        } else {
            //Channels.fireMessageReceived(context, result, remoteAddress);
            sequencerHandler.multicast((ChannelBuffer)result);
        }
    }

    private void cleanup(ChannelHandlerContext ctx, ChannelStateEvent e)
            throws Exception {
        try {
            ChannelBuffer cumulation = this.cumulation;
            //ChannelBuffer cumulation = this.cumulation.getAndSet(null);
            //if (cumulation == null) {
                //return;
            //}

            if (cumulation.readable()) {
                // Make sure all frames are read before notifying a closed channel.
                callDecode(ctx, ctx.getChannel(), cumulation, null);
            }

            // Call decodeLast() finally.  Please note that decodeLast() is
            // called even if there's nothing more to read from the buffer to
            // notify a user that the connection was closed explicitly.
            Object partialFrame = decodeLast(ctx, ctx.getChannel(), cumulation);
            if (partialFrame != null) {
                unfoldAndFireMessageReceived(ctx, null, partialFrame);
            }
        } finally {
            ctx.sendUpstream(e);
        }
    }

    private ChannelBuffer cumulation(ChannelHandlerContext ctx) {
        if (cumulation == null)
            cumulation = ChannelBuffers.dynamicBuffer(
                    ctx.getChannel().getConfig().getBufferFactory());
        return cumulation;
    }
    /*
    private ChannelBuffer cumulation(ChannelHandlerContext ctx) {
        ChannelBuffer buf = cumulation.get();
        if (buf == null) {
            buf = ChannelBuffers.dynamicBuffer(
                    ctx.getChannel().getConfig().getBufferFactory());
            if (!cumulation.compareAndSet(null, buf)) {
                buf = cumulation.get();
            }
        }
        return buf;
    }

    protected ChannelBuffer setcumu(ChannelBuffer cumu, ChannelBuffer input) {
            cumulation.set(ChannelBuffers.wrappedBuffer(cumu, input));
            return cumulation.get();
    }
    */
}

