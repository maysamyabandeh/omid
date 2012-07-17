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
import com.yahoo.omid.tso.messages.EndOfBroadcast;
import com.yahoo.omid.tso.messages.PeerIdAnnoncement;
import com.yahoo.omid.tso.TSOHandler;
import com.yahoo.omid.tso.TSOState;
import com.yahoo.omid.tso.TSOMessage;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.ChannelHandler;
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

/**
 * This client connects to the sequencer to register as a receiver of the broadcast stream
 * Note: This class is not thread-safe
 */
public class SequencerClient extends BasicClient {
    private static final Log LOG = LogFactory.getLog(TSOClient.class);

    //needed to create TSOHandler
    TSOState tsoState;
    ChannelGroup channelGroup;

    long lastReadIndex = 0;
    //for the moment use a dummy logBackendReader
    //it should be replaced by ManagedLedger when it is released
    LogBackendReader logBackendReader = new LogBackendReader() {
        @Override
        public void readRange(long fromIndex, long toIndex, ReadRangeCallback cb) {
            //read dummy but safe data
            System.out.println("readRange from backend : " + fromIndex + " " + toIndex);
            for (long lastReadIndex = fromIndex - 1; lastReadIndex < toIndex; ) {
                PeerIdAnnoncement dummyMsg = new PeerIdAnnoncement(-2);
                ChannelBuffer buffer = ChannelBuffers.buffer(20);
                buffer.writeByte(TSOMessage.PeerIdAnnoncement);
                dummyMsg.writeObject(buffer);
                lastReadIndex += buffer.readableBytes();
                byte[] dst = new byte[buffer.readableBytes()];
                buffer.readBytes(dst);
                cb.rangePartiallyRead(Code.OK, dst);
            }
            cb.rangeReadComplete(Code.OK, lastReadIndex);
        }
    };

    public SequencerClient(Properties conf, TSOState tsoState, ChannelGroup channelGroup) throws IOException {
        super(conf,0,false, null);
        //the id does not matter here (=0)
        this.tsoState = tsoState;
        this.channelGroup = channelGroup;
        registerForBroadcast(lastReadIndex);
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
    synchronized
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
        ctx.getChannel().getPipeline().remove("handler");
        ctx.getChannel().getPipeline().addLast("handler", new TSOHandler(channelGroup, tsoState) {
            @Override
            public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
                Object msg = e.getMessage();
                if (msg instanceof EndOfBroadcast) {
                    System.out.println(msg);
                    readGapFromLogBackend((EndOfBroadcast)msg);
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
                    super.messageReceived(ctx, e);
                }
            }

        });
        super.channelConnected(ctx, e);
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
                        //LOG.warn("rangePartiallyRead until " + lastReadIndex);
                        //feed the readData to the pipeline
                        //TODO: do it
                    }
                    @Override
                    public void rangeReadComplete(int rc, long lastReadIndex) {
                        LOG.warn("rangeReadComplete " + lastReadIndex);
                        //TODO: improvisation, remove it later
                        //cancel the effect of reads
                        SequencerClient.this.lastReadIndex = fromIndex - 1;
                        resumeBroadcast();
                    }
                });
    }

    void resumeBroadcast() {
        try {
            registerForBroadcast(lastReadIndex);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            //TODO: handle properly
        }
    }

    /**
     * When a message is received, handle it based on its type
     */
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        LOG.error("Unexpected message recieved at SequencerClient: " + e.getMessage());
        System.exit(1);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
    throws Exception {
        System.out.println("Unexpected exception " + e.getCause());
        e.getCause().printStackTrace();
    }
}


