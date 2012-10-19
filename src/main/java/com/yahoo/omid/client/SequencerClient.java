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

package com.yahoo.omid.client;

import com.yahoo.omid.tso.messages.BroadcastJoinRequest;
import com.yahoo.omid.tso.serialization.TSODecoder;
import com.yahoo.omid.tso.serialization.TSOEncoder;
import com.yahoo.omid.tso.messages.EndOfBroadcast;
import com.yahoo.omid.tso.TSOHandler;
import com.yahoo.omid.tso.TSOState;
import com.yahoo.omid.tso.TSOMessage;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.Channel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.io.IOException;
import java.util.Properties;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import com.yahoo.omid.tso.persistence.LoggerAsyncCallback.ReadRangeCallback;
import com.yahoo.omid.tso.persistence.LogBackendReader;
import com.yahoo.omid.tso.persistence.LoggerException.Code;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.iostream.IOStreamAddress;
import org.jboss.netty.channel.iostream.IOStreamChannelFactory;
import org.jboss.netty.bootstrap.ClientBootstrap;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.OutputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.DefaultChannelPipeline;

import com.yahoo.omid.tso.persistence.LoggerException;
import com.yahoo.omid.tso.persistence.LoggerException.Code;
import org.apache.zookeeper.ZooKeeper;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import com.yahoo.omid.tso.persistence.LoggerConstants;

import java.io.FileInputStream;
import java.io.File;
import java.io.FileNotFoundException;

/**
 * This client connects to the sequencer to register as a receiver of the broadcast stream
 * Note: This class is not thread-safe
 */
public class SequencerClient extends BasicClient {
    private static final Log LOG = LogFactory.getLog(TSOClient.class);

    //needed to create a new TSOHandler
    TSOState tsoState;
    ChannelGroup channelGroup;
    /**
     * a TSOHandler to service that messages are received through the sequencer
     */
    TSOHandler tsoHandler;
    /**
     * the loopback that is used to feed the read messages from the backend into
     * the netty pipeline
     */
    PipedOutputStream loopbackDataProvider = null;

    /**
     * TODO: this must be persisted.
     * Perhaps we can encode it as part of each entry that we regularly write to WAL.
     * This firstly reduce the WAL writing cost, and secondly make the recovery logic
     * simpler: a broadcast message should be rerun if the sideeffect of its execution
     * is not persisted.
     */
    long lastReadIndex = 0;

    /**
     * If the sequencer advances our speed to a level that it does not have the 
     * next broadcasted message in its history, we can read the missed messages
     * from the logBackend via this logBackendReader
     */
    LogBackendReader logBackendReader;

    /**
     * This class provides a backend reader based on files (perhaps over nfs)
     * TODO: replace it with a more reliable backend
     */
    private class SyncFileBackendReader implements LogBackendReader {
        FileInputStream file;
        /**
         * last read index by the cursor
         */
        long index = 0;

        public void init(String nfs, String fileName) throws LoggerException {
            final String path = nfs + "/" + fileName;
            File fileRef = new File(path);
            try {
                file = new FileInputStream(fileRef);
                LOG.warn("Successfully opened the file " + fileRef + " for reading");
            } catch (FileNotFoundException e) {
                LOG.error("File " + fileRef + " not found", e);
                e.printStackTrace();
                System.exit(1);
                //TODO: react better
            }
        }

        @Override
        public void readRange(long fromIndex, long toIndex, ReadRangeCallback cb) {
            System.out.println("readRange from backend : " + fromIndex + " " + toIndex);
            //1. seek to fromIndex
            while (index + 1 < fromIndex) {
                long seekStep = fromIndex - index - 1;
                try {
                    long skipped = file.skip(seekStep);
                    if (skipped != seekStep)
                        LOG.error("logBackend data is not ready: skiping " + seekStep + " and it skips only " + skipped);
                    index += skipped;
                    System.out.println("Skipping " + seekStep + " index is: " + index);
                } catch (IOException e) {
                    e.printStackTrace();
                    System.exit(1);
                    //TODO: react better
                }
            }
            //2. now read entries up to toIndex
            while (index < toIndex) {
                int readStep = (int)Math.min((toIndex - index), 1500);
                byte[] dst = new byte[readStep];
                try {
                    System.out.println("Reading " + readStep + " index is: " + index);
                    long actualRead = file.read(dst);
                    if (actualRead != readStep)
                        LOG.error("logBackend data is not ready: reading " + readStep + " and it reads only " + actualRead);
                    index += actualRead;
                    cb.rangePartiallyRead(Code.OK, dst);
                } catch (IOException e) {
                    e.printStackTrace();
                    System.exit(1);
                    //TODO: react better
                }
            }
            cb.rangeReadComplete(Code.OK, index);
        }
    }


    public SequencerClient(Properties conf, TSOState tsoState, ChannelGroup channelGroup) throws IOException {
        super(conf,0,false, null);
        //the id does not matter here (=0)
        this.tsoState = tsoState;
        this.channelGroup = channelGroup;
        this.tsoHandler = new TSOHandler(channelGroup, tsoState);
        initLoopbackChannel();
        registerForBroadcast(lastReadIndex);
        SyncFileBackendReader sfbr = new SyncFileBackendReader();
        try {
            sfbr.init(LoggerConstants.OMID_NFS_PATH, LoggerConstants.OMID_SEQUENCERLEDGER_ID_PATH);
            logBackendReader = sfbr;
        } catch (LoggerException e) {
            e.printStackTrace();
            System.exit(1);
            //TODO: react better
        }
    }

    /**
     * request to receive broadcast messages starting from lastReadIndex stream
     */
    void registerForBroadcast(long lastReadIndex) throws IOException {
        System.out.println("registerForBroadcast " + lastReadIndex);
        BroadcastJoinRequest msg = new BroadcastJoinRequest(lastReadIndex);
        withConnection(new MessageOp<BroadcastJoinRequest>(msg));
    }

    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e)
    throws Exception {
        resumeBroadcast();
    }

    /**
     * When a message is received, handle it based on its type
     */
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        Object msg = e.getMessage();
        if (msg instanceof EndOfBroadcast) {
            System.out.println(msg);
            readGapFromLogBackend((EndOfBroadcast)msg);
            //TODO: resume reading from the broadcaster concurrently
            /**
             * channel.setReadable(false);
             * resumeBroadcast()
             * and after reading from backend finishes ...
             * channel.setReadable(true);
             */
            //afterwards it resumes reading from the braodcaster
        } else {
            TSOMessage tsoMsg = (TSOMessage) msg;
            try {
                lastReadIndex += tsoMsg.size();
            } catch (Exception exp) {
                LOG.error("Error in getting the size of a TSOMessage: " + tsoMsg + exp);
                exp.printStackTrace();
            }
            //System.out.println("servicing message " + tsoMsg);
            tsoHandler.messageReceived(ctx, e);
        }
    }

    void initLoopbackChannel() {
        final ClientBootstrap loopbackBootstrap = new ClientBootstrap(new IOStreamChannelFactory(Executors.newCachedThreadPool()));
        loopbackBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() throws Exception {
                DefaultChannelPipeline pipeline = new DefaultChannelPipeline();
                pipeline.addLast("decoder", new TSODecoder());
                pipeline.addLast("encoder", new TSOEncoder());
                pipeline.addLast("handler", new TSOHandler(channelGroup, tsoState) {
                    @Override
                    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
                        TSOMessage tsoMsg = (TSOMessage) e.getMessage();
                        LOG.warn("Servicing message " + tsoMsg + " retrieved from the log backend");
                        try {
                            lastReadIndex += tsoMsg.size();
                        } catch (Exception exp) {
                            LOG.error("Error in getting the size of a TSOMessage: " + tsoMsg + exp);
                            exp.printStackTrace();
                        }
                        super.messageReceived(ctx, e);
                        resumeBroadcastIfRangeReadIsComplete();
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

    void readGapFromLogBackend(EndOfBroadcast msg) {
        final long fromIndex = lastReadIndex + 1;
        //since during reading from backend, the gap expands, we conservatively read
        //all the persisted data from the backend and then continute reading the
        //rest from the broadcaster
        final long toIndex = msg.lastAvailableIndex;
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
                    setLastReadIndexFromBackend(lastReadIndex);
                    resumeBroadcastIfRangeReadIsComplete();
                }
            });
    }

    /**
     * This has to be synchronized since could be called concurrently by both
     * (i) after servicing the last message
     * (ii) after rangeReadComplete
     * We need to invoke check after these two points since depends on 
     * concurrency either could be run first
     */
    synchronized
    void resumeBroadcastIfRangeReadIsComplete() {
        System.out.println("resumeBroadcastIfRangeReadIsComplete: " + lastReadIndex + " " + lastReadIndexFromBackend);
        if (lastReadIndex == lastReadIndexFromBackend) {
            lastReadIndexFromBackend = -1;//to cancel the next invocation of the method
            resumeBroadcast();
        }
    }

    long lastReadIndexFromBackend = -1;

    synchronized
    void setLastReadIndexFromBackend(long index) {
        lastReadIndexFromBackend = index;
    }

    void resumeBroadcast() {
        try {
            registerForBroadcast(lastReadIndex);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            //TODO: handle properly
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
    throws Exception {
        System.out.println("Unexpected exception " + e.getCause());
        e.getCause().printStackTrace();
    }
}


