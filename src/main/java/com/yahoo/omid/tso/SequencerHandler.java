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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Iterator;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;

import com.yahoo.omid.tso.TSOSharedMessageBuffer.ReadingBuffer;
import com.yahoo.omid.tso.messages.Peerable;
import com.yahoo.omid.tso.persistence.LoggerAsyncCallback.AddRecordCallback;
import com.yahoo.omid.tso.persistence.LoggerException;
import com.yahoo.omid.tso.persistence.LoggerException.Code;
import com.yahoo.omid.tso.persistence.LoggerProtocol;
import com.yahoo.omid.IsolationLevel;
import com.yahoo.omid.client.TSOClient;
import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.ScheduledExecutorService;

/**
 * ChannelHandler for the TSO Server
 * @author maysam
 *
 */
public class SequencerHandler extends SimpleChannelHandler {

    private static final Log LOG = LogFactory.getLog(SequencerHandler.class);
    static long BROADCAST_TIMEOUT = 1;

    TSOSharedMessageBuffer sharedMessageBuffer;

    /**
     * Bytes monitor
     */
    public static int globaltxnCnt = 0;

    ScheduledExecutorService broadcasters = null;

    /**
     * Channel Group
     */
    private ChannelGroup channelGroup = null;

    private Map<TSOClient, ReadingBuffer> messageBuffersMap = new HashMap<TSOClient, ReadingBuffer>();

    /**
     * The interface to the tsos
     */
    TSOClient[] tsoClients;

    /**
     * Constructor
     * @param channelGroup
     */
    public SequencerHandler(ChannelGroup channelGroup, TSOClient[] tsoClients) {
        this.broadcasters = Executors.newScheduledThreadPool(tsoClients.length);
        this.channelGroup = channelGroup;
        this.tsoClients = tsoClients;
        this.sharedMessageBuffer = new TSOSharedMessageBuffer(null);
        for (TSOClient tsoClient: tsoClients) {
            initReadBuffer(tsoClient);
        }
    }

    void initReadBuffer(TSOClient tsoClient) {
        ReadingBuffer buffer;
        synchronized (messageBuffersMap) {
            buffer = messageBuffersMap.get(tsoClient);
            if (buffer == null) {
                buffer = sharedMessageBuffer.new ReadingBuffer(tsoClient);
                messageBuffersMap.put(tsoClient, buffer);
                LOG.warn("init buffer for: " + tsoClient);
            } else {
                LOG.error("buffer already mapped to the tso! " + tsoClient);
            }
        }
        broadcasters.scheduleAtFixedRate(new BroadcastThread(buffer), 0, BROADCAST_TIMEOUT, TimeUnit.MILLISECONDS);
    }

    private class BroadcastThread implements Runnable {
        ReadingBuffer buffer;
        public BroadcastThread(ReadingBuffer buffer) {
            this.buffer = buffer;
        }
        @Override
        public void run() {
            synchronized (sharedMessageBuffer) {
                buffer.flush();
            }
        }
    }

    /**
     * If write of a message was not possible before, we can do it here
     */
    @Override
    public void channelInterestChanged(ChannelHandlerContext ctx,
            ChannelStateEvent e) {
    }

    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        channelGroup.add(ctx.getChannel());
    }

    /**
     * Handle receieved messages
     */
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        Object msg = e.getMessage();
        //if (msg instanceof Peerable && msg instanceof TSOMessage)
        multicast((TSOMessage)msg, ctx);
    }

    /**
     * Handle a received message
     * It has to be synchnronized to ensure atmoic broadcast
     */
    public void multicast(TSOMessage msg, ChannelHandlerContext ctx) {
        synchronized (sharedMessageBuffer) {
            sharedMessageBuffer.writeMessage(msg);
        }
        /*
        try {
            synchronized (tsoClients) {
                for (TSOClient tsoClient: tsoClients) {
                    tsoClient.forward(msg);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            //TODO: send nack back to client
        }
        */
    }

    private boolean finish;

    /*
     * Wrapper for Channel and Message
     */
    public static class ChannelandMessage {
        ChannelHandlerContext ctx;
        TSOMessage msg;
        ChannelandMessage(ChannelHandlerContext c, TSOMessage m) {
            ctx = c;
            msg = m;
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
        LOG.warn("TSOHandler: Unexpected exception from downstream.", e.getCause());
        e.getCause().printStackTrace();
        Channels.close(e.getChannel());
    }

    public void stop() {
        finish = true;
    }
}


