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

import com.yahoo.omid.tso.Committed;
import com.yahoo.omid.tso.RowKey;
import com.yahoo.omid.tso.Elder;
import com.yahoo.omid.tso.TSOMessage;
import com.yahoo.omid.tso.messages.AbortRequest;
import com.yahoo.omid.tso.messages.AbortedTransactionReport;
import com.yahoo.omid.tso.messages.CommitQueryRequest;
import com.yahoo.omid.tso.messages.CommitQueryResponse;
import com.yahoo.omid.tso.messages.CommitRequest;
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

public class TSOClient extends SimpleChannelHandler {
    private static final Log LOG = LogFactory.getLog(TSOClient.class);

    public static long askedTSO = 0;

    public enum Result {
        OK, ABORTED 
    };

    private Queue<PingPongCallback<TimestampResponse>> createCallbacks;
    private Map<Long, PingPongCallback<CommitResponse>> commitCallbacks;
    private Map<Long, PingPongCallback<PrepareResponse>> prepareCallbacks;
    private Map<Long, List<PingPongCallback<CommitQueryResponse>>> isCommittedCallbacks;

    private Committed committed = new Committed();
    SharedAbortedSet aborted = new SharedAbortedSet();
    //private Set<Long> aborted = Collections.synchronizedSet(new HashSet<Long>(1000));
    public Map<Long, Long> failedElders = Collections.synchronizedMap(new HashMap<Long, Long>(1000));
    private long largestDeletedTimestamp;
    //the txn id of the in-progress elder with lowest start timestamp
    private long eldest = 0;//by default, fetch all starting from 0
    private long connectionTimestamp = 0;
    private boolean hasConnectionTimestamp = false;

    private ChannelFactory factory;
    private ClientBootstrap bootstrap;
    private Channel channel;
    private InetSocketAddress addr;
    private int max_retries;
    private int retries;
    private int retry_delay_ms;
    private Timer retryTimer;

    private enum State {
        DISCONNECTED, CONNECTING, CONNECTED, RETRY_CONNECT_WAIT
    };

    private interface Op {
        public void execute(Channel channel);

        public void error(Exception e);
    }

    private class MessageOp<MSG extends TSOMessage> implements Op {
        MSG msg;

        MessageOp(MSG msg) {
            this.msg = msg;
        }

        public void execute(Channel channel) {
            try {
                ChannelFuture f = channel.write(msg);
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
        }

        public void error(Exception e) {
        }
    }

    private class SyncMessageOp<MSG extends TSOMessage> extends MessageOp<MSG> {
        Callback cb;

        SyncMessageOp(MSG msg, Callback cb) {
            super(msg);
            this.cb = cb;
        }
    }

    private ArrayBlockingQueue<Op> queuedOps;

    private State state;

    public final long getEldest() {
        return eldest;
    }

    public TSOClient(Properties conf) throws IOException {
        state = State.DISCONNECTED;
        queuedOps = new ArrayBlockingQueue<Op>(200);
        retryTimer = new Timer(true);

        commitCallbacks = Collections.synchronizedMap(new HashMap<Long, PingPongCallback<CommitResponse>>());
        prepareCallbacks = Collections.synchronizedMap(new HashMap<Long, PingPongCallback<PrepareResponse>>());
        isCommittedCallbacks = Collections.synchronizedMap(new HashMap<Long, List<PingPongCallback<CommitQueryResponse>>>());
        createCallbacks = new ConcurrentLinkedQueue<PingPongCallback<TimestampResponse>>();
        channel = null;

        System.out.println("Starting TSOClient");

        // Start client with Nb of active threads = 3 as maximum.
        factory = new NioClientSocketChannelFactory(Executors
                .newCachedThreadPool(), Executors.newCachedThreadPool(), 3);
        // Create the bootstrap
        bootstrap = new ClientBootstrap(factory);

        String tmp = conf.getProperty("tso.executor.threads", "3");
        int executorThreads = Integer.parseInt(tmp);

        bootstrap.getPipeline().addLast("executor", new ExecutionHandler(
                    new OrderedMemoryAwareThreadPoolExecutor(executorThreads, 1024*1024, 4*1024*1024)));
        bootstrap.getPipeline().addLast("handler", this);
        bootstrap.setOption("tcpNoDelay", false);
        bootstrap.setOption("keepAlive", true);
        bootstrap.setOption("reuseAddress", true);
        bootstrap.setOption("connectTimeoutMillis", 100);

        String host = conf.getProperty("tso.host");
        tmp = conf.getProperty("tso.port", "1234");
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

    private void withConnection(Op op) throws IOException {
        State state = connectIfNeeded();

        if (state == State.CONNECTING) {
            try {
                queuedOps.put(op);
            } catch (InterruptedException e) {
                throw new IOException("Couldn't add new operation", e);
            }
        } else if (state == State.CONNECTED) {
            op.execute(channel);
        } else {
            throw new IOException("Invalid connection state " + state);
        }
    }

    public void getNewTimestamp(PingPongCallback<TimestampResponse> cb) throws IOException {
        TimestampRequest tr = new TimestampRequest();
        getNewTimestamp(tr, cb);
    }

    public void getNewTimestamp(boolean trackProgress, PingPongCallback<TimestampResponse> cb) throws IOException {
        TimestampRequest tr = new TimestampRequest();
        tr.trackProgress = trackProgress;
        getNewTimestamp(tr, cb);
    }

    public void getNewTimestamp(long sequence, PingPongCallback<TimestampResponse> cb) throws IOException {
        TimestampRequest tr = new TimestampRequest();
        tr.sequence = sequence;
        getNewTimestamp(tr, cb);
    }

    public void getNewTimestamp(TimestampRequest tr, PingPongCallback<TimestampResponse> cb) throws IOException {
        synchronized(createCallbacks) {
            createCallbacks.add(cb);
        }
        withConnection(new SyncMessageOp<TimestampRequest>(tr, cb) {
            @Override
            public void error(Exception e) {
                synchronized(createCallbacks) {
                    createCallbacks.remove();
                }
                cb.error(e);
            }
        });
    }

    public void isCommitted(long startTimestamp, long pendingWriteTimestamp, PingPongCallback<CommitQueryResponse> cb)
        throws IOException {
        synchronized(isCommittedCallbacks) {
            List<PingPongCallback<CommitQueryResponse>> callbacks = isCommittedCallbacks.get(startTimestamp);
            if (callbacks == null) {
                callbacks = new ArrayList<PingPongCallback<CommitQueryResponse>>(1);
            }
            callbacks.add(cb);
            isCommittedCallbacks.put(startTimestamp, callbacks);
        }
        CommitQueryRequest qr = new CommitQueryRequest(startTimestamp, pendingWriteTimestamp);
        withConnection(new SyncMessageOp<CommitQueryRequest>(qr, cb) {
            @Override
            public void error(Exception e) {
                synchronized(isCommittedCallbacks) {
                    isCommittedCallbacks.remove(msg.startTimestamp);
                }
                cb.error(e);
            }
        });
    }

    public void abort(long transactionId) throws IOException {
        synchronized (commitCallbacks) {
            if (commitCallbacks.containsKey(transactionId)) {
                throw new IOException("Already committing transaction " + transactionId);
            }
        }
        AbortRequest ar = new AbortRequest();
        ar.startTimestamp = transactionId;
        withConnection(new MessageOp<AbortRequest>(ar));
    }

    private static RowKey[] EMPTY_ROWS = new RowKey[0]; 
    public void commit(long transactionId, CommitRequest msg, PingPongCallback<CommitResponse> cb) throws IOException {
        if (msg.writtenRows.length == 0) {
            msg.readRows = EMPTY_ROWS;
        }
        synchronized(commitCallbacks) {
            if (commitCallbacks.containsKey(transactionId)) {
                throw new IOException("Already committing transaction " + transactionId);
            }
            commitCallbacks.put(transactionId, cb); 
        }
        withConnection(new SyncMessageOp<CommitRequest>(msg, cb) {
            @Override
            public void error(Exception e) {
                synchronized(commitCallbacks) {
                    commitCallbacks.remove(msg.startTimestamp);
                }
                cb.error(e);
            }
        });
    }

    public void prepareCommit(long transactionId, PrepareCommit msg, PingPongCallback<PrepareResponse> cb) throws IOException {
        if (msg.writtenRows.length == 0) {
            msg.readRows = EMPTY_ROWS;
        }
        synchronized(prepareCallbacks) {
            if (prepareCallbacks.containsKey(transactionId)) {
                throw new IOException("Already preparing transaction " + transactionId);
            }
            prepareCallbacks.put(transactionId, cb); 
        }
        withConnection(new SyncMessageOp<PrepareCommit>(msg, cb) {
            @Override
            public void error(Exception e) {
                synchronized(prepareCallbacks) {
                    prepareCallbacks.remove(msg.startTimestamp);
                }
                cb.error(e);
            }
        });
    }

    public void completeAbort(long transactionId, PingCallback cb) throws IOException {
        FullAbortReport msg = new FullAbortReport();
        msg.startTimestamp = transactionId;
        withConnection(new SyncMessageOp<FullAbortReport>(msg, cb) {
            @Override
            public void error(Exception e) {
                cb.error(e);
            }
        });
    }

    //call this function after the reincarnation is complete
    //it sends a report to the tso
    public void completeReincarnation(long transactionId, PingCallback cb) throws IOException {
        ReincarnationReport msg = new ReincarnationReport();
        msg.startTimestamp = transactionId;
        withConnection(new SyncMessageOp<ReincarnationReport>(msg, cb) {
            @Override
            public void error(Exception e) {
                cb.error(e);
            }
        });
    }

    @Override
    synchronized
    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) {
        e.getChannel().getPipeline().addFirst("decoder", new TSODecoder());
        e.getChannel().getPipeline().addAfter("decoder", "encoder",
                new TSOEncoder());
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

    private void clearState() {
        committed = new Committed();
        aborted.clear();
        largestDeletedTimestamp = 0;
        connectionTimestamp = 0;
        hasConnectionTimestamp = false;
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

    public static final long INVALID_READ = -2;
    public static final long LOST_TC = -1;
    //In the new implementation, I need direct access to commit timestamp and the logic for deciding 
    // the committed version is more complex. Therefero, this function replaces validRead.
    // validRead could still be used if only validity of the version matters, like in tests
    public long commitTimestamp(long transaction, long startTimestamp) throws IOException {
        if (aborted.contains(transaction)) 
            return INVALID_READ;//invalid read
        long commitTimestamp = committed.getCommit(transaction);
        if (commitTimestamp != -1 && commitTimestamp > startTimestamp)
            return INVALID_READ;//invalid read
        if (commitTimestamp != -1 && commitTimestamp <= startTimestamp)
            return commitTimestamp;

        if (hasConnectionTimestamp && transaction > connectionTimestamp)
            return transaction <= largestDeletedTimestamp ? LOST_TC : INVALID_READ;
        //TODO: it works only if it runs one transaction at a time
        if (transaction <= largestDeletedTimestamp)
            return LOST_TC;//committed but the tc is lost

        Statistics.partialReport(Statistics.Tag.ASKTSO, 1);
        askedTSO++;
        PingPongCallback<CommitQueryResponse> cb = new PingPongCallback<CommitQueryResponse>();
        isCommitted(startTimestamp, transaction, cb);
        try {
            cb.await();
        } catch (InterruptedException e) {
            throw new IOException("Commit query didn't complete", e);
        }
        CommitQueryResponse pong = cb.getPong();
        if (pong.retry == true)///isNotAClearAnswer()
            //TODO: throw a proper exception
            throw new IOException("Either abort or retry the transaction");
        return pong.committed ? pong.commitTimestamp : INVALID_READ;
    }

    /**
     * When a message is received, handle it based on its type
     */
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("messageReceived " + e.getMessage());
        }
        Object msg = e.getMessage();
        if (msg instanceof CommitResponse) {
            CommitResponse r = (CommitResponse)msg;
            PingPongCallback<CommitResponse> cb = null;
            synchronized (commitCallbacks) {
                cb = commitCallbacks.remove(r.startTimestamp);
            }
            if (cb == null) {
                LOG.error("Received a commit response for a nonexisting commit");
                return;
            }
            cb.complete(r);
        } else if (msg instanceof PrepareResponse) {
            PrepareResponse r = (PrepareResponse)msg;
            PingPongCallback<PrepareResponse> cb = null;
            synchronized (prepareCallbacks) {
                cb = prepareCallbacks.remove(r.startTimestamp);
            }
            if (cb == null) {
                LOG.error("Received a prepare response for a nonexisting prepare");
                return;
            }
            cb.complete(r);
        } else if (msg instanceof TimestampResponse) {
            PingPongCallback<TimestampResponse> cb = createCallbacks.poll();
            long timestamp = ((TimestampResponse)msg).timestamp;
            if (!hasConnectionTimestamp || timestamp < connectionTimestamp) {
                hasConnectionTimestamp = true;
                connectionTimestamp = timestamp;
            }
            if (cb == null) {
                LOG.error("Receiving a timestamp response, but none requested: " + timestamp);
                return;
            }
            if (((TimestampResponse)msg).isFailed()) {
                //System.out.println("failed");
                cb.error(new Exception("out of order sequence"));
            }
            else
                cb.complete((TimestampResponse)msg);
        } else if (msg instanceof CommitQueryResponse) {
            CommitQueryResponse r = (CommitQueryResponse)msg;
            if (r.commitTimestamp != 0) {
                committed.commit(r.queryTimestamp, r.commitTimestamp);
            } else if (r.committed) {
                committed.commit(r.queryTimestamp, largestDeletedTimestamp);
            }
            List<PingPongCallback<CommitQueryResponse>> cbs = null;
            synchronized (isCommittedCallbacks) {
                cbs = isCommittedCallbacks.remove(r.startTimestamp);
            }
            if (cbs == null) {
                LOG.error("Received a commit query response for a nonexisting request");
                return;
            }
            for (PingPongCallback<CommitQueryResponse> cb : cbs) {
                cb.complete(r);
            }
        } else if (msg instanceof CommittedTransactionReport) {
            CommittedTransactionReport ctr = (CommittedTransactionReport) msg;
            committed.commit(ctr.startTimestamp, ctr.commitTimestamp);
            //Always add (Tc, Tc) as well since some transactions might be elders and reinsert their written data
            committed.commit(ctr.commitTimestamp, ctr.commitTimestamp);
        } else if (msg instanceof FullAbortReport) {
            FullAbortReport r = (FullAbortReport) msg;
            aborted.remove(r.startTimestamp);
        } else if (msg instanceof FailedElderReport) {
            FailedElderReport r = (FailedElderReport) msg;
            failedElders.put(r.startTimestamp, r.commitTimestamp);
            LOG.warn("Client: " + r);
        } else if (msg instanceof EldestUpdate) {
            EldestUpdate r = (EldestUpdate) msg;
            eldest = r.startTimestamp;
            //LOG.warn("Client: " + r);
        } else if (msg instanceof ReincarnationReport) {
            ReincarnationReport r = (ReincarnationReport) msg;
            Long Tc = failedElders.remove(r.startTimestamp);
            boolean res = Tc != null;
            LOG.warn("Client: " + res + " " + r);
        } else if (msg instanceof AbortedTransactionReport) {
            AbortedTransactionReport r = (AbortedTransactionReport) msg;
            aborted.add(r.startTimestamp);
        } else if (msg instanceof LargestDeletedTimestampReport) {
            LargestDeletedTimestampReport r = (LargestDeletedTimestampReport) msg;
            largestDeletedTimestamp = r.largestDeletedTimestamp;
            committed.raiseLargestDeletedTransaction(r.largestDeletedTimestamp);
        } else {
            LOG.error("Unknown message received " +  msg);
        }
        processMessage((TSOMessage) msg);
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
        synchronized (createCallbacks) {
            for (PingPongCallback<TimestampResponse> cb : createCallbacks) {
                cb.error(e);
            }
            createCallbacks.clear();
        }

        synchronized(commitCallbacks) {
            for (PingPongCallback<CommitResponse> cb : commitCallbacks.values()) {
                cb.error(e);
            } 
            commitCallbacks.clear();
        }

        synchronized(isCommittedCallbacks) {
            for (List<PingPongCallback<CommitQueryResponse>> cbs : isCommittedCallbacks.values()) {
                for (PingPongCallback<CommitQueryResponse> cb : cbs) {
                    cb.error(e);
                }
            } 
            isCommittedCallbacks.clear();
        }      
    }

    protected void processMessage(TSOMessage msg) {
        // TODO Auto-generated method stub
    }

}
