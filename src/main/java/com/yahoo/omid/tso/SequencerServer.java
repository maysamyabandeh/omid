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

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.Properties;
import java.io.IOException;

import com.yahoo.omid.OmidConfiguration;
import com.yahoo.omid.client.TSOClient;
import org.apache.hadoop.conf.Configuration;

import org.jboss.netty.channel.ChannelHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.serialization.ObjectDecoder;
import org.jboss.netty.handler.codec.serialization.ObjectEncoder;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;


/**
 * Sequencer Server
 */
public class SequencerServer extends TSOServer {
    
    private static final Log LOG = LogFactory.getLog(SequencerServer.class);

    /**
     * The interface to the tsos
     */
    TSOClient[] tsoClients;

    public SequencerServer(TSOServerConfig config, Properties[] soConfs) {
        super(config);
        tsoClients = new TSOClient[soConfs.length];
        try {
            for (int i = 0; i < soConfs.length; i++) {
                final int id = 0;//the id does not matter here
                tsoClients[i] = new TSOClient(soConfs[i], 0);
            }
        } catch (IOException e) {
            LOG.error("SO is not available " + e);
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    /**
     * Take two arguments :<br>
     * -port to listen to<br>
     * -nb of connections before shutting down
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        TSOServerConfig config = TSOServerConfig.parseConfig(args);
        OmidConfiguration omidConf = OmidConfiguration.create();
        omidConf.loadServerConfs(config.getZkServers());
        new SequencerServer(config, omidConf.getStatusOracleConfs()).run();
    }

    @Override
    protected ChannelHandler newMessageHandler() {
        return new SequencerHandler(channelGroup, tsoClients);
    }

    @Override
    protected void stopHandler(ChannelHandler handler) {
        //((SequencerHandler)handler).stop();
    }
}

