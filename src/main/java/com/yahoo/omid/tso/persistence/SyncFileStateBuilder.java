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


package com.yahoo.omid.tso.persistence;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.util.concurrent.CountDownLatch;

import com.yahoo.omid.tso.messages.FullAbortReport;
import com.yahoo.omid.tso.messages.AbortedTransactionReport;
import com.yahoo.omid.tso.messages.LargestDeletedTimestampReport;
import com.yahoo.omid.tso.messages.CommittedTransactionReport;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.yahoo.omid.tso.TSOServerConfig;
import com.yahoo.omid.tso.TSOState;
import com.yahoo.omid.tso.TimestampOracle;
import com.yahoo.omid.tso.persistence.LoggerAsyncCallback.BuilderInitCallback;
import com.yahoo.omid.tso.persistence.LoggerAsyncCallback.LoggerInitCallback;
import com.yahoo.omid.tso.persistence.LoggerException.Code;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.MessageEvent;
import com.yahoo.omid.tso.TSOMessage;
import com.yahoo.omid.tso.persistence.LoggerAsyncCallback.ReadRangeCallback;
import com.yahoo.omid.tso.TSOHandler;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.DefaultChannelPipeline;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.iostream.IOStreamAddress;
import org.jboss.netty.channel.iostream.IOStreamChannelFactory;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import com.yahoo.omid.tso.serialization.TSODecoder;
import com.yahoo.omid.tso.serialization.TSOEncoder;
import com.yahoo.omid.tso.Zipper;
import java.io.OutputStream;

/**
 * Builds the TSO state from a file if there has been a previous 
 * incarnation of TSO. Note that we need to make sure that the zookeeper session 
 * is the same across builder and logger, so we create in builder and pass it
 * to logger. This is the case to prevent two TSO instances from creating a lock
 * and updating the ledger id after having lost the lock. This case would
 * lead to an invalid system state.
 *
 * @author maysam
 *
 */

public class SyncFileStateBuilder extends StateBuilder {
    private static final Log LOG = LogFactory.getLog(SyncFileStateBuilder.class);

    /**
     * The start index of the last snapshot
     * The log is read only from this point on
     * TODO: optimize by setting it properly
     */
    long snapshotIndex = 0;

    public static TSOState getState(TSOServerConfig config){
        TSOState returnValue;
        SyncFileStateBuilder builder = new SyncFileStateBuilder(config);
        try{
            returnValue = builder.buildState();
            LOG.info("State built");
        } catch (Throwable e) {
            LOG.error("Error while building the state.", e);
            returnValue = null;
        } finally {
            builder.shutdown();
        }
        return returnValue;
    }

    TSOState tsoState;
    TimestampOracle timestampOracle;
    long fileSize = 0;
    CountDownLatch latch = new CountDownLatch(1);
    TSOServerConfig config;

    @Override
    public TSOState buildState() throws LoggerException { 
        //The buildState is not implemented for isolation levels that do not check for write-write conflicts. We can remove this exception throwing, after implementing that.
        //TODO: do it
        if (!com.yahoo.omid.IsolationLevel.checkForWriteWriteConflicts)
            throw LoggerException.create(Code.LOGGERDISABLED);

        this.timestampOracle = new TimestampOracle();
        //this.config = config;
        this.tsoState = new TSOState(timestampOracle);
        initLoopbackChannel();
        SyncFileBackendReader sfbr = new SyncFileBackendReader();
        try {
            fileSize = sfbr.init(LoggerConstants.OMID_NFS_PATH, LoggerConstants.OMID_TSO_LOG + config.getId());
            logBackendReader = sfbr;
        } catch (FileNotFoundException e) {
            LOG.error(e.getMessage());
            LOG.error("No log exist to recover the state from--It is ok if you run it for the first time");
            return tsoState;
        } catch (LoggerException e) {
            e.printStackTrace();
            System.exit(1);
            //TODO: react better
        }
        long toIndex = fileSize - snapshotIndex - 1;//index starts from 0
        readGapFromLogBackend(snapshotIndex, toIndex);
        try {
            latch.await();
        } catch (Exception e) {
            LOG.error("Exception while recovery from the log file", e);
            throw LoggerException.create(Code.ILLEGALOP);
        }
        return tsoState;
    }

    /**
     * the loopback that is used to feed the read messages from the backend into
     * the netty pipeline
     */
    PipedOutputStream loopbackDataProvider = null;

    /**
     * we can read from the logBackend via this logBackendReader
     */
    LogBackendReader logBackendReader;

    SyncFileStateBuilder(TSOServerConfig config) {
        this.config = config;
    }

    void initLoopbackChannel() {
        final ClientBootstrap loopbackBootstrap = new ClientBootstrap(new IOStreamChannelFactory(Executors.newCachedThreadPool()));
        loopbackBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() throws Exception {
                ChannelGroup channelGroup = new DefaultChannelGroup(SyncFileStateBuilder.class.getName());
                DefaultChannelPipeline pipeline = new DefaultChannelPipeline();
                pipeline.addLast("decoder", new TSODecoder(new Zipper()));
                pipeline.addLast("encoder", new TSOEncoder());
                pipeline.addLast("handler", new TSOHandler(channelGroup, tsoState) {
                    @Override
                    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
                        TSOMessage tsoMsg = (TSOMessage) e.getMessage();
                        //LOG.warn("Servicing message " + tsoMsg + " retrieved from the log backend");
                        processLogMessage(tsoMsg);
                    }
                });
                return pipeline;
            }
        });
        final PipedInputStream loopbackSrc = new PipedInputStream();
        loopbackDataProvider = null;
        try {
            loopbackDataProvider = new PipedOutputStream(loopbackSrc);
        } catch (IOException e) {
            LOG.error("error in creating loopback pipeline!");
            e.printStackTrace();
            System.exit(1);
            //TODO: handle properly
        }
        // Make a new connection.
        ChannelFuture connectFuture = loopbackBootstrap.connect(
                new IOStreamAddress(
                    loopbackSrc,
                    new OutputStream() {
                        @Override
                        public void write(int b) throws IOException {
                            throw new IOException("Data received at /dev/null");
                        }
                    })
                );
        // Wait until the connection is made successfully.
        final Channel loopbackChannel = connectFuture.awaitUninterruptibly().getChannel();
    }

    void readGapFromLogBackend(final long fromIndex, final long toIndex) {
        logBackendReader.readRange(
            fromIndex,
            toIndex,
            new ReadRangeCallback() {
                @Override
                public void rangePartiallyRead(int rc, byte[] readData) {
                    LOG.warn("rangePartiallyRead size: " + readData.length);
                    //feed the readData to the pipeline
                    try {
                        loopbackDataProvider.write(readData, 0, readData.length);
                    } catch (IOException e) {
                        LOG.error("loopback pipeline is broken!");
                        e.printStackTrace();
                        System.exit(1);
                        //TODO: handle properly
                    }
                }
                @Override
                public void rangeReadComplete(int rc, long lastReadIndex) {
                    LOG.warn("rangeReadComplete " + lastReadIndex);
                    try {
                        loopbackDataProvider.flush();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    latch.countDown();// resume normal service
                }
            });
    }

    /**
     * The logged messaged are fed to this function in order
     */
    void processLogMessage(TSOMessage msg) {
        if (msg instanceof FullAbortReport) {
            tsoState.processFullAbort(((FullAbortReport)msg).startTimestamp);
        } else if (msg instanceof AbortedTransactionReport) {
            tsoState.processAbort(((AbortedTransactionReport)msg).startTimestamp);
        } else if (msg instanceof LargestDeletedTimestampReport) {
            tsoState.processLargestDeletedTimestamp(((LargestDeletedTimestampReport)msg).largestDeletedTimestamp);
        } else if (msg instanceof CommittedTransactionReport) {
            CommittedTransactionReport ctr = (CommittedTransactionReport) msg;
            tsoState.processCommit(ctr.startTimestamp, ctr.commitTimestamp);
        } else {
            LOG.error("Unkown message in the log: " + msg);
            System.exit(1);
            //TODO: exit is for debugging. remove it later
        }
    }

    /**
     * Disables this builder.
     */
    @Override
    public void shutdown(){
    }

}

