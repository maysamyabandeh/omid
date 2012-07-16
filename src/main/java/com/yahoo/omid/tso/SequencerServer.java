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

import com.yahoo.omid.tso.serialization.SeqDecoder;
import com.yahoo.omid.tso.serialization.TSODecoder;
import com.yahoo.omid.tso.serialization.TSOEncoder;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.Properties;
import java.io.IOException;
import java.util.concurrent.Executor;

import com.yahoo.omid.OmidConfiguration;
import com.yahoo.omid.client.TSOClient;
import org.apache.hadoop.conf.Configuration;

import org.jboss.netty.handler.execution.ExecutionHandler;
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
import org.apache.zookeeper.ZooKeeper;


/**
 * Sequencer Server
 */
public class SequencerServer  implements Runnable {

    private static final Log LOG = LogFactory.getLog(SequencerServer.class);

    private TSOServerConfig config;
    private boolean finish = false;
    private Object lock = new Object();
    protected ChannelGroup channelGroup = new DefaultChannelGroup(SequencerServer.class.getName());

    ZooKeeper zk;

    public SequencerServer(TSOServerConfig config, OmidConfiguration omidConf, Properties[] soConfs, ZooKeeper zk) {
        super();
        this.config = config;
        this.omidConf = omidConf;
        this.zk = zk;
    }

    OmidConfiguration omidConf = null;

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
        ZooKeeper zk = new ZooKeeper(config.getZkServers(), 
                Integer.parseInt(System.getProperty("SESSIONTIMEOUT", Integer.toString(10000))), 
                null);
        new SequencerServer(config, omidConf, omidConf.getStatusOracleConfs(), zk).run();
    }

    SequencerHandler sequencerHandler;

    public static int SEQ_REGISTER_PORT_SHIFT = 2;
    @Override
    public void run() {
        int maxSocketThreads = 1;
        ChannelFactory factory = new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool(),
                maxSocketThreads);
        ServerBootstrap bootstrap = new ServerBootstrap(factory);
        // Create the global ChannelGroup
        channelGroup = new DefaultChannelGroup(SequencerServer.class.getName());
        //More concurrency gives lower performance due to synchronizations
        int maxThreads = omidConf.getInt("tso.maxthread", 4);
        System.out.println("maxThreads: " + maxThreads);
        // Memory limitation: 1MB by channel, 1GB global, 100 ms of timeout
        ThreadPoolExecutor pipelineExecutor = new OrderedMemoryAwareThreadPoolExecutor(maxThreads, 1048576, 1073741824, 100, TimeUnit.MILLISECONDS, Executors.defaultThreadFactory());

        sequencerHandler = new SequencerHandler(channelGroup, zk, omidConf.getStatusOracleConfs().length);
        //ChannelPipelineFactory pipelineFactory =  new SeqPipelineFactory(pipelineExecutor, sequencerHandler);
        final ExecutionHandler executionHandler = new ExecutionHandler(pipelineExecutor);
        ChannelPipelineFactory pipelineFactory =  new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipeline = Channels.pipeline();
                pipeline.addLast("decoder", new SeqDecoder());
                pipeline.addLast("pipelineExecutor", executionHandler);
                pipeline.addLast("handler", sequencerHandler);
                return pipeline;
            }
        };
        bootstrap.setPipelineFactory(pipelineFactory);
        bootstrap.setOption("tcpNoDelay", false);
        bootstrap.setOption("child.tcpNoDelay", false);
        bootstrap.setOption("child.keepAlive", true);
        bootstrap.setOption("child.reuseAddress", true);
        bootstrap.setOption("child.connectTimeoutMillis", 60000);
        bootstrap.setOption("readWriteFair", true);

        //Register a pipeline that accepts registration for the broadcast service
        ChannelGroup registerGroup = new DefaultChannelGroup("register");
        ChannelFactory factory2 = new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool(),
                maxSocketThreads);
        ServerBootstrap registerBootstrap = new ServerBootstrap(factory2);
        // Memory limitation: 10K by channel, 100K global, 100 ms of timeout
        ThreadPoolExecutor registerPipelineExecutor = new OrderedMemoryAwareThreadPoolExecutor(1, 10*1024, 100*1024, 100, TimeUnit.MILLISECONDS, Executors.defaultThreadFactory());
        registerBootstrap.setOption("tcpNoDelay", false);
        registerBootstrap.setOption("child.tcpNoDelay", false);
        registerBootstrap.setOption("child.keepAlive", true);
        registerBootstrap.setOption("child.reuseAddress", true);
        registerBootstrap.setOption("child.connectTimeoutMillis", 60000);
        registerBootstrap.setOption("readWriteFair", true);
        final ExecutionHandler registerExecutionHandler = new ExecutionHandler(registerPipelineExecutor);
        registerBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
           @Override
           public ChannelPipeline getPipeline() throws Exception {
               System.out.println("creating a new pipeline");
              ChannelPipeline pipeline = Channels.pipeline();
              pipeline.addLast("decoder", new TSODecoder());
              pipeline.addLast("pipelineExecutor", registerExecutionHandler);
              pipeline.addLast("handler", sequencerHandler);
              return pipeline;
           }
        });
        Channel channel = registerBootstrap.bind(new InetSocketAddress(config.getPort() + SEQ_REGISTER_PORT_SHIFT));
        registerGroup.add(channel);

        // Create the monitor
        ThroughputMonitor monitor = new ThroughputMonitor(null);
        // Add the parent channel to the group
        channel = bootstrap.bind(new InetSocketAddress(config.getPort()));
        channelGroup.add(channel);

        // Starts the monitor
        monitor.start();
        synchronized (lock) {
            while (!finish) {
                try {
                    lock.wait();
                } catch (InterruptedException e) {
                    break;
                }
            }
        }

        //stopHandler(handler);//handler.stop();

        // *** Start the Netty shutdown ***
        // End the monitor
        System.out.println("End of monitor");
        monitor.interrupt();
        // Now close all channels
        System.out.println("End of channel group");
        channelGroup.close().awaitUninterruptibly();
        // Close the executor for Pipeline
        System.out.println("End of pipeline executor");
        pipelineExecutor.shutdownNow();
        registerPipelineExecutor.shutdownNow();
        // Now release resources
        System.out.println("End of resources");
        factory.releaseExternalResources();
    }

    public void stop() {
        finish = true;
    }
}
