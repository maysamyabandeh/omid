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

import org.jboss.netty.channel.ChannelHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.io.IOException;
import java.util.Properties;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.ChannelStateEvent;

/**
 * This client connects to the sequencer to register as a receiver of the broadcast stream
 */
public class SequencerClient extends BasicClient {
    private static final Log LOG = LogFactory.getLog(TSOClient.class);
    ChannelHandler handler;

    public SequencerClient(Properties conf, ChannelHandler handler) throws IOException {
        super(conf,0,true, null);
        //the id does not matter here (=0)
        this.handler = handler;
    }

    @Override
    synchronized
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
        ctx.getChannel().getPipeline().remove("handler");
        ctx.getChannel().getPipeline().addLast("handler", handler);
        super.channelConnected(ctx, e);
    }

    /**
     * When a message is received, handle it based on its type
     */
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        LOG.error("Unexpected message recieved at SequencerClient: " + e.getMessage());
        System.exit(1);
    }
}


