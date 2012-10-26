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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelFuture;


import com.yahoo.omid.tso.Zipper;
import com.yahoo.omid.tso.Committed;
import com.yahoo.omid.tso.RowKey;
import com.yahoo.omid.tso.Elder;
import com.yahoo.omid.tso.TSOMessage;
import com.yahoo.omid.tso.messages.AbortRequest;
import com.yahoo.omid.tso.messages.PeerIdAnnoncement;
import com.yahoo.omid.tso.messages.AbortedTransactionReport;
import com.yahoo.omid.tso.messages.CommitQueryRequest;
import com.yahoo.omid.tso.messages.CommitQueryResponse;
import com.yahoo.omid.tso.messages.CommitRequest;
import com.yahoo.omid.tso.messages.MultiCommitRequest;
import com.yahoo.omid.tso.messages.CommitResponse;
import com.yahoo.omid.tso.messages.PrepareCommit;
import com.yahoo.omid.tso.messages.PrepareResponse;
import com.yahoo.omid.tso.messages.CommittedTransactionReport;
import com.yahoo.omid.tso.messages.FullAbortReport;
import com.yahoo.omid.tso.messages.ReincarnationReport;
import com.yahoo.omid.tso.messages.EldestUpdate;
import com.yahoo.omid.tso.messages.FailedElderReport;
import com.yahoo.omid.tso.messages.LargestDeletedTimestampReport;
import com.yahoo.omid.tso.messages.TimestampRequest;
import com.yahoo.omid.tso.messages.TimestampResponse;
import com.yahoo.omid.tso.serialization.TSODecoder;
import com.yahoo.omid.tso.serialization.TSOEncoder;
import com.yahoo.omid.Statistics;
import java.util.concurrent.atomic.AtomicLong;
import org.jboss.netty.channel.ChannelHandler;

public class BasicClient extends SimpleChannelHandler implements Comparable<BasicClient> {
    private static final Log LOG = LogFactory.getLog(BasicClient.class);

    private ChannelFactory factory;
    private ClientBootstrap bootstrap;
    Channel channel;
    InetSocketAddress addr;
    private int max_retries;
    private int retries;
    private int retry_delay_ms;
    private Timer retryTimer;

    private enum State {
        DISCONNECTED, CONNECTING, CONNECTED, RETRY_CONNECT_WAIT
    };

    private interface Op {
        public ChannelFuture execute(Channel channel);

        public void error(Exception e);
    }

    private class BufferOp implements Op {
        ChannelBuffer msg;

        BufferOp(ChannelBuffer msg) {
            this.msg = msg;
        }

        public ChannelFuture execute(Channel channel) {
            ChannelFuture f = null;
            try {
                f = channel.write(msg);
                f.addListener(new ChannelFutureListener() {
                    public void operationComplete(ChannelFuture future) {
                        if (!future.isSuccess()) {
                            error(new IOException("Error writing to socket"));
                        }
                    }
                });
            } catch (Exception e) {
                error(e);
            }
            return f;
        }

        public void error(Exception e) {
        }
    }

    protected class MessageOp<MSG extends TSOMessage> implements Op {
        MSG msg;

        MessageOp(MSG msg) {
            this.msg = msg;
        }

        public ChannelFuture execute(Channel channel) {
            ChannelFuture f = null;
            try {
                f = channel.write(msg);
                f.addListener(new ChannelFutureListener() {
                    public void operationComplete(ChannelFuture future) {
                        if (!future.isSuccess()) {
                            error(new IOException("Error writing to socket"));
                        }
                    }
                });
            } catch (Exception e) {
                error(e);
            }
            return f;
        }

        public void error(Exception e) {
        }
    }

    protected class SyncMessageOp<MSG extends TSOMessage> extends MessageOp<MSG> {
        Callback cb;

        SyncMessageOp(MSG msg, Callback cb) {
            super(msg);
            this.cb = cb;
        }
    }

    private ArrayBlockingQueue<Op> queuedOps;

    private State state;

    /**
     * The id that uniquely identifies the TSOClient accross all the TSOClients
     */
    final int myId;

    public BasicClient(Properties conf) throws IOException {
        this(conf,0,false, null);
    }

    public BasicClient(Properties conf, int id, boolean introduceYourself, ChannelHandler handler) throws IOException {
        myId = id;
        state = State.DISCONNECTED;
        queuedOps = new ArrayBlockingQueue<Op>(200);
        retryTimer = new Timer(true);

        channel = null;
        // Start client with Nb of active threads = 3 as maximum.
        factory = new NioClientSocketChannelFactory(Executors
                .newCachedThreadPool(), Executors.newCachedThreadPool(), 3);
        // Create the bootstrap
        bootstrap = new ClientBootstrap(factory);
        //In the client that is connect to a single channel, we dont need an executor
        //String tmp = conf.getProperty("tso.executor.threads", "3");
        //int executorThreads = Integer.parseInt(tmp);
        //bootstrap.getPipeline().addLast("executor", new ExecutionHandler(
                    //new OrderedMemoryAwareThreadPoolExecutor(executorThreads, 1024*1024, 4*1024*1024)));
        bootstrap.getPipeline().addLast("handler", handler != null ? handler : this);
        bootstrap.getPipeline().addFirst("decoder", new TSODecoder(new Zipper()));
        bootstrap.getPipeline().addAfter("decoder", "encoder", new TSOEncoder());
        bootstrap.setOption("tcpNoDelay", false);
        bootstrap.setOption("keepAlive", true);
        bootstrap.setOption("reuseAddress", true);
        bootstrap.setOption("connectTimeoutMillis", 100);

        String host = conf.getProperty("tso.host");
        String tmp = conf.getProperty("tso.port", "1234");
        System.out.println("Starting BasicClient to " + host + " " + tmp);
        int port = Integer.parseInt(tmp);
        tmp = conf.getProperty("tso.max_retries", "10");
        max_retries = Integer.parseInt(tmp);
        tmp = conf.getProperty("tso.retry_delay_ms", "3000"); 
        retry_delay_ms = Integer.parseInt(tmp);

        if (host == null) {
            throw new IOException("tso.host missing from configuration");
        }

        addr = new InetSocketAddress(host, port);
        connectIfNeeded();
        if (introduceYourself)
            introducePeerId();
    }

    private State connectIfNeeded() throws IOException {
        synchronized (state) {
            if (state == State.CONNECTED || state == State.CONNECTING) {
                return state;
            }
            if (state == State.RETRY_CONNECT_WAIT) {
                return State.CONNECTING;
            }

            if (retries > max_retries) {
                IOException e = new IOException("Max connection retries exceeded");
                bailout(e);
                throw e;
            }
            retries++;
            bootstrap.connect(addr);
            state = State.CONNECTING;
            return state;
        }
    }

    protected ChannelFuture withConnection(Op op) throws IOException {
        State state = connectIfNeeded();

        if (state == State.CONNECTING) {
            try {
                queuedOps.put(op);
            } catch (InterruptedException e) {
                throw new IOException("Couldn't add new operation", e);
            }
        } else if (state == State.CONNECTED) {
            return op.execute(channel);
        } else {
            throw new IOException("Invalid connection state " + state);
        }
        return null;//TODO: we need a different kind of listener that does not depend on channel
    }

    /**
     * Forward a message
     * do not wait for the respond
     */
    public void forward(TSOMessage msg) throws IOException {
        withConnection(new MessageOp<TSOMessage>(msg));
    }
    public ChannelFuture forward(ChannelBuffer buf) throws IOException {
        return withConnection(new BufferOp(buf));
    }

    /**
     * Send the id to the peer of the connection
     */
    public void introducePeerId() throws IOException {
        PeerIdAnnoncement msg = new PeerIdAnnoncement(myId);
        withConnection(new MessageOp<PeerIdAnnoncement>(msg));
    }

    @Override
    synchronized
    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) {
        //e.getChannel().getPipeline().addFirst("decoder", new TSODecoder(new Zipper()));
        //e.getChannel().getPipeline().addAfter("decoder", "encoder", new TSOEncoder());
    }

    /**
     * Starts the traffic
     */
    @Override
    synchronized
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
        synchronized (state) {
            channel = e.getChannel();
            state = State.CONNECTED;
            retries = 0;
        }
        clearState();
        Op o = queuedOps.poll();;
        while (o != null && state == State.CONNECTED) {
            o.execute(channel);
            o = queuedOps.poll();
        }
    }

    protected void clearState() {
    }

    @Override
    synchronized
    public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e)
    throws Exception {
    synchronized(state) {
        channel = null;
        state = State.DISCONNECTED;
    }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx,
            ExceptionEvent e)
    throws Exception {
    System.out.println("Unexpected exception " + e.getCause());
    e.getCause().printStackTrace();

    synchronized(state) {

        if (state == State.CONNECTING) {
            state = State.RETRY_CONNECT_WAIT;
            if (LOG.isTraceEnabled()) {
                LOG.trace("Retrying connect in " + retry_delay_ms + "ms " + retries);
            }
            try {
                retryTimer.schedule(new TimerTask() {
                    public void run() {
                        synchronized (state) {
                            state = State.DISCONNECTED;
                            try {
                                connectIfNeeded();
                            } catch (IOException e) {
                                bailout(e);
                            }
                        }
                    }
                }, retry_delay_ms);
            } catch (Exception cause) {
                bailout(cause);
            }
        } else {
            LOG.error("Exception on channel", e.getCause());
        }
    }
    }

    public void bailout(Exception cause) {
        synchronized (state) {
            state = State.DISCONNECTED;
        }
        LOG.error("Unrecoverable error in client, bailing out", cause);
        Exception e = new IOException("Unrecoverable error", cause);
        Op o = queuedOps.poll();;
        while (o != null) {
            o.error(e);
            o = queuedOps.poll();
        }
    }

    static protected int generateUniqueId() {
        java.util.Random rnd;
        long seed = System.currentTimeMillis();
        seed *= Thread.currentThread().getId();// to make it thread dependent
        rnd = new java.util.Random(seed);
        int rndInt = rnd.nextInt();
        return generateUniqueId(rndInt);
    }

    static protected int generateUniqueId(int i) {
        int id = 0;
        try {
            InetAddress thisIp = InetAddress.getLocalHost();
            id = thisIp.hashCode();
            id = id * (i+1);//avoid i = 0
            LOG.warn("Generate Id: " + id);
            return id;
        } catch (UnknownHostException e) {
            LOG.error("Error in generating the unique id" + e);
            e.printStackTrace();
            System.exit(1);
        }
        return id;
    }

    @Override
    public int compareTo(BasicClient other) {
        return this.channel.compareTo(other.channel);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof BasicClient))
            return false;
        BasicClient tsoobj = (BasicClient) obj;
        return tsoobj.addr.equals(addr);
    }

    @Override
    public int hashCode() {
        return addr.hashCode();
    }

    @Override
    public String toString() {
        return channel == null ? "null" : channel.toString();
    }

}

