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

import com.yahoo.omid.tso.messages.PeerIdAnnoncement;
import com.yahoo.omid.tso.messages.BroadcastJoinRequest;
import com.yahoo.omid.tso.messages.EndOfBroadcast;
import com.yahoo.omid.tso.persistence.LoggerAsyncCallback.AddRecordCallback;
import com.yahoo.omid.tso.persistence.LoggerException;
import com.yahoo.omid.tso.persistence.LoggerException.Code;
import com.yahoo.omid.tso.persistence.LoggerAsyncCallback.LoggerInitCallback;
import com.yahoo.omid.IsolationLevel;
import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.ScheduledExecutorService;
import com.yahoo.omid.sharedlog.*;
import com.yahoo.omid.tso.persistence.StateLogger;
import com.yahoo.omid.tso.persistence.SyncFileStateLogger;
import org.apache.zookeeper.ZooKeeper;

/**
 * ChannelHandler for the Sequencer Server
 * This class implements atomic broadcast.
 * It receives messages from the clients and broadcast them to the registered 
 * status oracles.
 * @author maysam
 */
public class SequencerHandler extends SimpleChannelHandler {

    private static final Log LOG = LogFactory.getLog(SequencerHandler.class);
    static long BROADCAST_TIMEOUT = 1;

    /**
     * The sharedLog is a lock-free log that keeps the recent sequenced messages
     * The readers read messages from this log and send it to the registered SOs
     */
    SharedLog sharedLog;

    /**
     * Bytes monitor
     * Used only for statistical purposes
     */
    public static int globaltxnCnt = 0;

    /**
     * We have a thread for sending data to each registered status oracle.
     * This thread is called a boradcaster
     */
    ScheduledExecutorService broadcasters = null;

    /**
     * Channel Group
     */
    private ChannelGroup channelGroup = null;

    /**
     * We keep a mapping between channels and logreaders.
     * This is used when an already registered status oracle, request registering 
     * for the broadcast service.
     * TODO: not sure if it is needed assuming correct status oracles
     */
    private Map<Channel, LogReader> channelToReaderMap = new HashMap<Channel, LogReader>();

    /**
     * There is a single writer for the log. The append method is synchronized to give
     * a serial order to the messages.
     */
    LogWriter logWriter;

    /**
     * logPersister persists the content of the sharedLog into a logBackend
     */
    LogPersister logPersister;

    /**
     * the logBackend provides persistent storage for the log content. The logBackend
     * must be accessible by braodcast registeres, i.e., status oracles.
     */
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
        //logWriter should be careful not to rewrite the data that is not persisted yet.
        this.logWriter.setPersister(this.logPersister);
        //1 statistics printing thread
        broadcasters.scheduleAtFixedRate(
                new Runnable() {
                    @Override
                    public void run() {
                        Statistics.println();
                    }
                }, 1, 3000, TimeUnit.MILLISECONDS);
        //1 persisting thread
        broadcasters.schedule(new PersistenceThread(logPersister), 0, TimeUnit.MILLISECONDS);
    }

    /**
     * Init the logBackend medium
     * The users of logBackend are notified of the init completion by checking
     * that logBackend is not null
     */
    void initLogBackend(ZooKeeper zk) {
        try {
            new SyncFileStateLogger(zk).initialize(new LoggerInitCallback() {
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
            //TODO: react properly
            System.exit(1);
        }
    }

    /**
     * Assign a reader thread to the channel that is registering for broadcast
     */
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

    /**
     * A separate BroadcastThread is assigned to each registered status oracle
     * This thread regularly reads from the sharedlog and push the new content to 
     * the channel of the assigned status oracle
     */
    private class BroadcastThread implements Runnable {
        Channel channel;
        LogReader logReader;
        //Need the schedulerControler pointer to cancel the broadcast
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
                Thread.sleep(5);//allow the writes to accumulate
                //TODO: replace the voodo number 5
                //inject random errors: used for testing
                //if (error()) return;
                ChannelBuffer tail = logReader.tail();
                if (tail == null)
                    return;
                //used for statistical purposes
                //flush++;
                //flushsize += tail.readableBytes();
                //System.out.println("Braodcasting " + tail.readableBytes() + " from " + logReader);
                channel.write(tail);
            } catch (SharedLogLateFollowerException lateE) {
                //lateE.printStackTrace();
                sendEOB(channel);
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

        /**
         * stop the braodcast
         * in this version, we cleanly anounce the end of broadcast
         */
        void sendEOB(Channel channel) {
            boolean result = stopBroadcastingTo(channel);
            //if we cannot stop broadcasting, sending EOB messes with semantics
            if (!result)
                return;
            final long suggestIndexForResume = logPersister.getGlobalPointer();
            System.out.println("sending EOB: suggesting " + suggestIndexForResume);
            EndOfBroadcast eob = new EndOfBroadcast(suggestIndexForResume);
            //encode the message
            //TODO: do it in a clean way
            ChannelBuffer buffer = ChannelBuffers.buffer(20);
            buffer.writeByte(TSOMessage.EndOfBroadcast);
            eob.writeObject(buffer);
            //send the message
            channel.write(buffer);
        }

        /**
         * stop broadcast
         * In this version, we close the channel to emulate a channel failure
         */
        void simulateFailure(Channel channel) {
            boolean result = stopBroadcastingTo(channel);
            //if we cannot stop broadcasting, sending EOB messes with semantics
            if (!result)
                return;
            System.out.println("breaking the connection " + channel);
            try {
                channel.close();
            } catch (Exception exp) {
                exp.printStackTrace();
            }
        }

        /**
         * Inject an error: used for testing purposes
         * @return true if the error is injected
         */
        boolean error() {
            if (!sent && logPersister.getGlobalPointer() > 50000) {
                sendEOB(channel);
                SequencerHandler.sent = true;
                return true;
            }
            if (!sent2 && logPersister.getGlobalPointer() > 100000) {
                //activate it for the second time
                SequencerHandler.sent = false;
                SequencerHandler.sent2 = true;
            }
            return false;
        }
    }
    //used for testing purposes
    static boolean sent = false;
    static boolean sent2 = false;

    /**
     * A thread that periodically reads from the sharedLog that persists the recent
     * writes of the sharedLog
     */
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
                    //keep count for statistical purposes
                    TSOHandler.globaltxnCnt++;
                    ChannelBuffer tail = toBePersistedData.getData();
                    //TODO: update the logBackend to operate on ChannelBuffer
                    //this would eliminate the extra copy
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
                                        //update the pointer of the last persisted data
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
        //} else if (!(msg instanceof ChannelBuffer)) {
            //System.out.println("WRONG MSG: " + msg);
            //System.out.println("channel: " + ctx.getChannel());
        } else //coming form clients, broadcast it
            multicast((ChannelBuffer)msg);
    }

    long writeCnt = 0;
    long writeSize = 0;
    /**
     * Handle a received message
     * logWriter.append is synchnronized to ensure atmoic broadcast
     */
    public void multicast(ChannelBuffer buf) {
        writeCnt ++;
        writeSize += buf.readableBytes();
        //used for statistical purposes
        //_totalWritesInBytes = writeSize;
        //_averageWriteSize = writeSize / (float) writeCnt;
        try {
            TSOHandler.txnCnt++;
            //append is synchronized to serialize the messages
            logWriter.append(buf);
        } catch (SharedLogException sharedE) {
            //TODO do something
            sharedE.printStackTrace();
        }
    }

    /**
     * could be used to nicely stop the entire braodcast
     * not used yet
     */
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

