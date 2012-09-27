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
import com.yahoo.omid.tso.messages.PeerIdAnnoncement;
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

import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenLedgerCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.Position;
import com.yahoo.omid.tso.persistence.LoggerException;
import com.yahoo.omid.tso.persistence.LoggerException.Code;
import org.apache.zookeeper.ZooKeeper;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import com.yahoo.omid.tso.persistence.LoggerConstants;
import org.apache.bookkeeper.mledger.Entry;

/**
 * This client connects to the sequencer to register as a receiver of the broadcast stream
 * Note: This class is not thread-safe
 */
public class SequencerClient extends BasicClient {
    private static final Log LOG = LogFactory.getLog(TSOClient.class);

    //needed to create TSOHandler
    TSOState tsoState;
    ChannelGroup channelGroup;
    TSOHandler tsoHandler;
    PipedOutputStream loopbackDataProvider = null;

    /**
     * TODO: this must be persisted.
     * Perhaps we can encode it as part of each entry that we regularly write to WAL.
     * This firstly reduce the WAL writing cost, and secondly make the recovery logic
     * simpler: a broadcast message should be rerun if the sideeffect of its execution
     * is not persisted.
     */
    long lastReadIndex = 0;

    LogBackendReader logBackendReader = new LogBackendReader() {
    //for the moment use a dummy logBackendReader
    //it should be replaced by ManagedLedger when it is released
        @Override
        public void readRange(long fromIndex, long toIndex, ReadRangeCallback cb) {
            //read dummy but safe data
            System.out.println("readRange from backend : " + fromIndex + " " + toIndex);
            long index;
            for (index = fromIndex - 1; index < toIndex; ) {
                PeerIdAnnoncement dummyMsg = new PeerIdAnnoncement(-2);
                ChannelBuffer buffer = ChannelBuffers.buffer(20);
                buffer.writeByte(TSOMessage.PeerIdAnnoncement);
                dummyMsg.writeObject(buffer);
                index += buffer.readableBytes();
                byte[] dst = new byte[buffer.readableBytes()];
                buffer.readBytes(dst);
                cb.rangePartiallyRead(Code.OK, dst);
            }
            cb.rangeReadComplete(Code.OK, index);
        }
    };

    /**
     * This class provide a backend reader based on managed ledgers on bookkeeper
     * It is very inefficient and should be replaced with a proper backend
     * TODO: replace it with a proper backend
     */
    private class ManagedLedgerBackendReader implements LogBackendReader {
        ManagedCursor cursor;
        /**
         * last read index by the cursor
         */
        long index = 0;
        public void init(String zkServers) throws LoggerException {
            ZooKeeper zk;
            BookKeeper bk;
            try {
                zk = new ZooKeeper(zkServers, 
                        Integer.parseInt(System.getProperty("SESSIONTIMEOUT", Integer.toString(10000))), 
                        null);
                bk = new BookKeeper(new ClientConfiguration(), zk);
            } catch (Exception e) {
                LOG.error("Exception while initializing bookkeeper", e);
                e.printStackTrace();
                throw new LoggerException.BKOpFailedException();  
            }

            ManagedLedgerFactory factory;
            try {
                factory = new ManagedLedgerFactoryImpl(bk, zk);
            } catch (Exception e) {
                LOG.error("Exception while creating managed ledger bookkeeper", e);
                e.printStackTrace();
                throw new LoggerException.BKOpFailedException();  
            }

            final String path = LoggerConstants.OMID_SEQUENCERLEDGER_ID_PATH;
            try {
                ManagedLedger ledger = factory.open(path);
                LOG.warn("Successfully opened the managed ledger " + path);
                cursor = ledger.openCursor("reader" + myId);
            } catch (Exception exception) {
                LOG.error("Failed to open the managed ledger " + path, exception );
                exception.printStackTrace();
            };
        }

        /**
         * Since BK stores entries, we need to make a mapping between entries and 
         * the byte indexes
         */
        @Override
        public void readRange(long fromIndex, long toIndex, ReadRangeCallback cb) {
            System.out.println("readRange from backend : " + fromIndex + " " + toIndex);
            //1. seek to fromIndex
            /**
             * TODO: this is a very inefficient seek, either upgrade managed ledger
             * or replace bookkeeper
             */
            SEEK:
            while (index < fromIndex) {
                //assert(cursor.hasMoreEntries());
                int seekStep = (int) ((fromIndex - index) / 1000);//1000 is estimated entry size
                seekStep = Math.max(1, seekStep);
                System.out.println("seekStep : " + seekStep + " " + fromIndex + " " + index + " " + toIndex);
                java.util.List<Entry> entries = null;
                try {
                    entries = cursor.readEntries(seekStep);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(1);
                    //TODO: react better
                }
                System.out.println("parse entries[" + entries.size() + "]");
                if (entries.size() == 0)
                    System.exit(1);
                for (Entry entry: entries) {
                    index += entry.getData().length;
                    System.out.println("entry : " + entry.getData().length + " " + fromIndex + " " + index + " " + toIndex);
                    if (index >= fromIndex) {
                        index -= entry.getData().length;//cancel the entry read effect
                        cursor.seek(entry.getPosition());
                        //we want to start reading from this entry
                        break SEEK;
                    }
                }
            }
            //2. now read entries up to toIndex
            long nextIndex = -1;
LOGREAD:
            while (index < toIndex) {
                //assert(cursor.hasMoreEntries());
                int readStep = (int)((toIndex - index) / 1000);//1000 is estimated entry size
                readStep = Math.max(1, readStep);
                System.out.println("readStep : " + readStep + " " + fromIndex + " " + index + " " + toIndex);
                java.util.List<Entry> entries = null;
                try {
                    entries = cursor.readEntries(readStep);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(1);
                    //TODO: react better
                }
                System.out.println("parse entries[" + entries.size() + "]");
                for (Entry entry: entries) {
                    nextIndex = index + entry.getData().length;
                    if (index >= fromIndex && nextIndex < toIndex) {
                        cb.rangePartiallyRead(Code.OK, entry.getData());
                    } else {
                        byte[] data = entry.getData();
                        ChannelBuffer buffer = ChannelBuffers.copiedBuffer(data,
                                (int) Math.max(0, fromIndex - index),
                                (int) Math.min(toIndex - index, data.length));
                        byte[] dst = new byte[buffer.readableBytes()];
                        buffer.readBytes(dst);
                        cb.rangePartiallyRead(Code.OK, dst);
                    }
                    System.out.println("entry : " + entry.getData().length + " " + fromIndex + " " + index + " " + toIndex);
                    if (nextIndex >= toIndex) {
                        cursor.seek(entry.getPosition());
                        break LOGREAD;
                    } else
                        index = nextIndex;
                }
            }
            cb.rangeReadComplete(Code.OK, nextIndex);
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
        ManagedLedgerBackendReader mlbr = new ManagedLedgerBackendReader();
        try {
            mlbr.init(conf.getProperty("zkServers"));
        } catch (LoggerException e) {
            e.printStackTrace();
            System.exit(1);
            //TODO: react better
        }
        //logBackendReader = mlbr;
    }

    /**
     * Send the id to the peer of the connection
     */
    void registerForBroadcast(long lastReadIndex) throws IOException {
        System.out.println("registerForBroadcast " + lastReadIndex);
        BroadcastJoinRequest msg = new BroadcastJoinRequest(lastReadIndex);
        withConnection(new MessageOp<BroadcastJoinRequest>(msg));
    }

    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e)
    throws Exception {
        lastReadIndexFromBroadcaster = lastReadIndex;
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

    //TODO: improvisation to cancel the effect of fake reads from backend
    //remove it after implementing reading from a backend
    long lastReadIndexFromBroadcaster;

    void readGapFromLogBackend(EndOfBroadcast msg) {
        lastReadIndexFromBroadcaster = lastReadIndex;
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
     * We need to invoke resume check after these two points since depends on 
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
        //TODO: improvisation, remove it later
        //cancel the effect of reads
        //lastReadIndex = lastReadIndexFromBroadcaster;
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


