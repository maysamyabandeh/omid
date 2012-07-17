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

import com.yahoo.omid.Statistics;

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
import com.yahoo.omid.tso.messages.PeerIdAnnoncement;
import com.yahoo.omid.tso.messages.BroadcastJoinRequest;
import com.yahoo.omid.tso.messages.EndOfBroadcast;
import com.yahoo.omid.tso.persistence.LoggerAsyncCallback.AddRecordCallback;
import com.yahoo.omid.tso.persistence.LoggerException;
import com.yahoo.omid.tso.persistence.LoggerException.Code;
import com.yahoo.omid.tso.persistence.LoggerProtocol;
import com.yahoo.omid.tso.persistence.LoggerAsyncCallback.LoggerInitCallback;
import com.yahoo.omid.IsolationLevel;
import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.ScheduledExecutorService;
import com.yahoo.omid.sharedlog.*;
import com.yahoo.omid.tso.persistence.StateLogger;
import com.yahoo.omid.tso.persistence.BookKeeperStateLogger;
import org.apache.zookeeper.ZooKeeper;

/**
 * ChannelHandler for the TSO Server
 * @author maysam
 *
 */
public class SequencerHandler extends SimpleChannelHandler {

    private static final Log LOG = LogFactory.getLog(SequencerHandler.class);
    static long BROADCAST_TIMEOUT = 1;

    SharedLog sharedLog;

    /**
     * Bytes monitor
     */
    public static int globaltxnCnt = 0;

    ScheduledExecutorService broadcasters = null;

    /**
     * Channel Group
     */
    private ChannelGroup channelGroup = null;

    private Map<Channel, LogReader> channelToReaderMap = new HashMap<Channel, LogReader>();

    LogWriter logWriter;
    LogPersister logPersister;
    StateLogger logBackend;

    /**
     * Constructor
     * @param channelGroup
     */
    public SequencerHandler(ChannelGroup channelGroup, ZooKeeper zk, final int numberOfSOs) {
        this.broadcasters = Executors.newScheduledThreadPool(numberOfSOs + 1 + 1);
        // + 1 persiter + 1 statistics
        this.channelGroup = channelGroup;
        //this.tsoClients = tsoClients;
        this.sharedLog = new SharedLog();
        this.logWriter = new LogWriter(sharedLog);
        initLogBackend(zk);
        this.logPersister = new LogPersister(sharedLog, logWriter);
        this.logWriter.setPersister(this.logPersister);
        broadcasters.scheduleAtFixedRate(
                new Runnable() {
                    @Override
                    public void run() {
                        Statistics.println();
                    }
                }, 1, 3000, TimeUnit.MILLISECONDS);
        broadcasters.schedule(new PersistenceThread(logPersister), 0, TimeUnit.MILLISECONDS);
    }

    void initLogBackend(ZooKeeper zk) {
        try {
            new BookKeeperStateLogger(zk).initialize(new LoggerInitCallback() {
                public void loggerInitComplete(int rc, StateLogger sl, Object ctx){
                    if(rc == Code.OK){
                        if(LOG.isDebugEnabled()){
                            LOG.debug("Logger is ok.");
                        }
                        LOG.warn("backend loggerInitComplete: OK");
                        logBackend = sl;
                    } else {
                        LOG.error("Error when initializing logger: " + LoggerException.getMessage(rc));
                    }
                }

            }, null);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    void initReader(Channel channel, BroadcastJoinRequest msg, FollowedPointer subject) {
        LogReader logReader;
        synchronized (channelToReaderMap) {
            logReader = channelToReaderMap.get(channel);
            if (logReader != null)
                LOG.error("reader already mapped to the tso! " + channel);
            logReader = new LogReader(sharedLog, subject, msg.lastRecievedIndex);
            channelToReaderMap.put(channel, logReader);
            LOG.warn("init reader for: " + channel);
        }
        BroadcastThread broadcastThread = new BroadcastThread(channel, logReader);
        final ScheduledFuture<?> schedulerControler = broadcasters.scheduleAtFixedRate(broadcastThread, 0, BROADCAST_TIMEOUT, TimeUnit.MILLISECONDS);
        broadcastThread.setControler(schedulerControler);
    }

    private class BroadcastThread implements Runnable {
        Channel channel;
        LogReader logReader;
        ScheduledFuture<?> schedulerControler;
        public BroadcastThread(Channel channel, LogReader logReader) {
            this.channel = channel;
            this.logReader = logReader;
        }
        public void setControler(ScheduledFuture<?> schedulerControler) {
            this.schedulerControler = schedulerControler;
        }
        @Override
        public void run() {
            try {
                if (!channel.isConnected()) {
                    LOG.error("Broadcast channel is not connected");
                    stopBroadcastingTo(channel);
                    return;
                }
                ChannelBuffer tail = logReader.tail();
                if (tail == null)
                    return;
                TSOSharedMessageBuffer._flushes++;
                TSOSharedMessageBuffer._flSize += tail.readableBytes();
                System.out.println("Braodcasting " + tail.readableBytes() + " from " + logReader);
                //System.out.println("(" + schedulerControler == null ? "null" : schedulerControler.isCancelled() + ") " + "Braodcasting " + tail.readableBytes() + " from " + logReader);
                channel.write(tail);
                //TODO: this is for test, must be removed later
                sendEOB(channel);
            } catch (SharedLogLateFollowerException lateE) {
                //TODO do something
                lateE.printStackTrace();
            } catch (SharedLogException sharedE) {
                //TODO do something
                sharedE.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
                stopBroadcastingTo(channel);
            }
        }

        boolean stopBroadcastingTo(Channel channel) {
            if (schedulerControler == null) {
                LOG.error("No contoller set to stop the broadcast: " + channel);
                return false;
            }
            LOG.error("Stop broadcasting to channel: " + channel);
            schedulerControler.cancel(false);
            return true;
        }

        void sendEOB(Channel channel) {
            boolean result = stopBroadcastingTo(channel);
            //if we cannot stop broadcasting, sending EOB messes with semantics
            if (!result)
                return;
            final long suggestIndexForResume = logPersister.getGlobalPointer();
            System.out.println("sending EOB: suggesting " + suggestIndexForResume);
            EndOfBroadcast eob = new EndOfBroadcast(suggestIndexForResume);
            ChannelBuffer buffer = ChannelBuffers.buffer(20);
            buffer.writeByte(TSOMessage.EndOfBroadcast);
            eob.writeObject(buffer);
            channel.write(buffer);
        }
    }

    private class PersistenceThread implements Runnable {
        LogPersister logPersister;
        public PersistenceThread(LogPersister logPersister) {
            this.logPersister = logPersister;
        }
        @Override
        public void run() {
            for (;;) {
                try {
                    if (logBackend == null) {
                        System.out.println("Wait more for the log backend ...");
                        Thread.sleep(100);
                        continue;
                    }
                    LogPersister.ToBePersistedData toBePersistedData = logPersister.toBePersisted();
                    if (toBePersistedData == null) {
                        Thread.yield();
                        continue;
                    }
                    ChannelBuffer tail = toBePersistedData.getData();
                    byte[] record;
                    record = new byte[tail.readableBytes()];
                    tail.readBytes(record);
                    logBackend.addRecord(record, 
                            new AddRecordCallback() {
                                @Override
                                public void addRecordComplete(int rc, Object ctx) {
                                    if (rc != Code.OK) {
                                        LOG.error("Writing to log backend failed: " + LoggerException.getMessage(rc));
                                        System.exit(1);
                                        //TODO: handle it properly
                                    } else {
                                        LogPersister.ToBePersistedData toBePersistedData = (LogPersister.ToBePersistedData) ctx;
                                        toBePersistedData.persisted();
                                    }
                                }
                            }, toBePersistedData);
                    Thread.sleep(1);
                } catch (SharedLogLateFollowerException lateE) {
                    //TODO do something
                    lateE.printStackTrace();
                } catch (SharedLogException sharedE) {
                    //TODO do something
                    sharedE.printStackTrace();
                //} catch (IOException ioE) {
                    ////TODO do something
                    //ioE.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
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
     * This handle could be called both by the client new messgeas to be broadcasted
     * and by TSOServers to be registered for broadcasts
     */
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        Object msg = e.getMessage();
        if (msg instanceof BroadcastJoinRequest) {//coming from TSO to register
            initReader(ctx.getChannel(), (BroadcastJoinRequest)msg, logPersister);
        } else
            multicast((ChannelBuffer)msg);
    }

    long writeCnt = 0;
    long writeSize = 0;
    /**
     * Handle a received message
     * It has to be synchnronized to ensure atmoic broadcast
     */
    public void multicast(ChannelBuffer buf) {
        writeCnt ++;
        writeSize += buf.readableBytes();
        TSOSharedMessageBuffer._Avg2 = writeSize / (float) writeCnt;
        TSOSharedMessageBuffer._Writes = writeSize;
        try {
            logWriter.append(buf);
        } catch (SharedLogException sharedE) {
            //TODO do something
            sharedE.printStackTrace();
        }
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
        LOG.warn("SequencerHandler: Unexpected exception from downstream.", e.getCause());
        e.getCause().printStackTrace();
        Channels.close(e.getChannel());
    }

    public void stop() {
        finish = true;
    }
}

