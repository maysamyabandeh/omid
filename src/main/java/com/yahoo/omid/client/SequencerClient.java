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
                if (msg instanceof EndOfBroadcast)
                    resumeBroadcast();
                else {
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

    void resumeBroadcast() {
        //TODO: resume properly
        System.out.println("End of Broadcast");
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


