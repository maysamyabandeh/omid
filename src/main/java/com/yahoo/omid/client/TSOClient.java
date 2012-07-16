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
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelFuture;


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

public class TSOClient extends BasicClient {
    private static final Log LOG = LogFactory.getLog(TSOClient.class);

    public static long askedTSO = 0;

    private Queue<PingPongCallback<TimestampResponse>> createCallbacks;
    private Map<Long, PingPongCallback<TimestampResponse>> sequencedTimestampCallbacks;
    private Map<Long, PingPongCallback<CommitResponse>> commitCallbacks;
    private Map<Long, PingPongCallback<PrepareResponse>> prepareCallbacks;
    private Map<Long, List<PingPongCallback<CommitQueryResponse>>> isCommittedCallbacks;

    private Committed committed = new Committed();
    SharedAbortedSet aborted = new SharedAbortedSet();
    public Map<Long, Long> failedElders = Collections.synchronizedMap(new HashMap<Long, Long>(1000));
    private long largestDeletedTimestamp;
    //the txn id of the in-progress elder with lowest start timestamp
    private long eldest = 0;//by default, fetch all starting from 0
    private long connectionTimestamp = 0;
    private boolean hasConnectionTimestamp = false;

    //The sequenc generator must be shared among all TSOClients run by a client
    protected AtomicLong sequenceGenerator;// = new AtomicLong();

    public final long getEldest() {
        return eldest;
    }

    public TSOClient(Properties conf, AtomicLong sequenceGenerator) throws IOException {
        this(conf,0,false, sequenceGenerator);
    }

    public TSOClient(Properties conf) throws IOException {
        this(conf,0,false, null);
    }

    public TSOClient(Properties conf, int id, boolean introduceYourself) throws IOException {
        this(conf, id, introduceYourself, null);
    }

    public TSOClient(Properties conf, int id, boolean introduceYourself, AtomicLong sequenceGenerator) throws IOException {
        super(conf, id, introduceYourself, null);
        this.sequenceGenerator = sequenceGenerator;
        commitCallbacks = Collections.synchronizedMap(new HashMap<Long, PingPongCallback<CommitResponse>>());
        prepareCallbacks = Collections.synchronizedMap(new HashMap<Long, PingPongCallback<PrepareResponse>>());
        isCommittedCallbacks = Collections.synchronizedMap(new HashMap<Long, List<PingPongCallback<CommitQueryResponse>>>());
        createCallbacks = new ConcurrentLinkedQueue<PingPongCallback<TimestampResponse>>();
        sequencedTimestampCallbacks = Collections.synchronizedMap(new HashMap<Long, PingPongCallback<TimestampResponse>>());
    }

    public void getNewTimestamp(PingPongCallback<TimestampResponse> cb) throws IOException {
        TimestampRequest tr = new TimestampRequest();
        getNewTimestamp(tr, cb);
    }

    /**
     * the readonly timestmap requests set a falg in the server, so
     * they should be differentiated from normal timestmap requests
     * a readonly flag in response is enough but to be uniform 
     * I am using the already existing sequence
     */
    public void getNewTimestamp(PingPongCallback<TimestampResponse> cb, long sequence) throws IOException {
        TimestampRequest tr = new TimestampRequest();
        tr.trackProgress = false;
        tr.sequence = sequence;
        synchronized(sequencedTimestampCallbacks) {
            sequencedTimestampCallbacks.put(sequence, cb);
        }
        withConnection(new SyncMessageOp<TimestampRequest>(tr, cb) {
            @Override
            public void error(Exception e) {
                synchronized(sequencedTimestampCallbacks) {
                    sequencedTimestampCallbacks.remove(msg.getSequence());
                }
                cb.error(e);
            }
        });
    }

    /**
     * Register a callback for future timestmp response
     * This is useful for indirect communications, when the reponse is triggered
     * by an indirect message
     */
    public PingPongCallback<TimestampResponse> registerTimestampCallback(long sequence) throws IOException {
        PingPongCallback<TimestampResponse> cb = new PingPongCallback();
        synchronized(sequencedTimestampCallbacks) {
            sequencedTimestampCallbacks.put(sequence, cb);
        }
        return cb;
    }

    /**
     * Ask for a timestamp through a middle node
     * Use client id to specify the peer and a sequence to specify the request
     */
    public void getNewIndirectTimestamp(long sequence) throws IOException {
        TimestampRequest tr = new TimestampRequest();
        tr.peerId = myId;
        tr.sequence = sequence;
        tr.globalTxn = true;
        withConnection(new MessageOp<TimestampRequest>(tr));
    }
    public void getNewIndirectTimestamp(long sequence, boolean readOnly) throws IOException {
        TimestampRequest tr = new TimestampRequest();
        tr.peerId = myId;
        tr.sequence = sequence;
        //readOnly transactions do not need the so to keep track of their commit
        tr.trackProgress = !readOnly;
        tr.globalTxn = true;
        withConnection(new MessageOp<TimestampRequest>(tr));
    }

    /**
     * Ask for a timestamp without a sequence
     * The response could be used for any previous timestamp requests
     */
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
                    commitCallbacks.remove(msg.getStartTimestamp());
                }
                cb.error(e);
            }
        });
    }

    /**
     * Register a callback for future commit response
     * This is useful for indirect communications, when the reponse is triggered
     * by an indirect message
     */
    public PingPongCallback<CommitResponse> registerCommitCallback(long transactionId) throws IOException {
        PingPongCallback<CommitResponse> cb = new PingPongCallback();
        synchronized(commitCallbacks) {
            if (commitCallbacks.containsKey(transactionId)) {
                throw new IOException("Already committing transaction " + transactionId);
            }
            commitCallbacks.put(transactionId, cb); 
        }
        return cb;
    }

    /**
     * Ask for a commit timestamp through a middle node
     * Use client id to specify the peer and a sequence to specify the request
     */
    public void getNewIndirectCommitTimestamp(MultiCommitRequest msg) throws IOException {
        if (msg.writtenRows.length == 0) {
            msg.readRows = EMPTY_ROWS;
        }
        msg.peerId = myId;
        withConnection(new MessageOp<MultiCommitRequest>(msg));
    }

    public void prepareCommit(long transactionId, PrepareCommit msg, PingPongCallback<PrepareResponse> cb) throws IOException {
        if (msg.writtenRows.length == 0) {
            msg.readRows = EMPTY_ROWS;
        }
        msg.peerId = myId;
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

    /**
     * It is possible that clearState is invoked before the constructor is called
     */
    @Override
    protected void clearState() {
        committed = new Committed();
        if (aborted != null)
            aborted.clear();
        largestDeletedTimestamp = 0;
        connectionTimestamp = 0;
        hasConnectionTimestamp = false;
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
                LOG.error("Received a commit response for a not-requested commit " + r);
                return;
            }
            if (r.isFailed()) {
                cb.error(new Exception("out of order sequence"));
            }
            else
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
            TimestampResponse tr = (TimestampResponse)msg;
            if (!hasConnectionTimestamp || tr.timestamp < connectionTimestamp) {
                hasConnectionTimestamp = true;
                connectionTimestamp = tr.timestamp;
            }
            PingPongCallback<TimestampResponse> cb;
            if (tr.isSequenced())
                synchronized (sequencedTimestampCallbacks) {
                    cb = sequencedTimestampCallbacks.remove(tr.getSequence());
                }
            else
                synchronized (createCallbacks) {
                    cb = createCallbacks.poll();
                }
            if (cb == null) {
                LOG.error("Receiving a timestamp response, but none requested: " + tr);
                return;
            }
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
        processMessage((TSOMessage)msg);
    }

    protected void processMessage(TSOMessage msg) {
        //let it be overridden by the subclasses
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

    @Override
    public void bailout(Exception cause) {
        super.bailout(cause);
        Exception e = new IOException("Unrecoverable error", cause);
        synchronized (createCallbacks) {
            for (PingPongCallback<TimestampResponse> cb : createCallbacks) {
                cb.error(e);
            }
            createCallbacks.clear();
        }
        synchronized (sequencedTimestampCallbacks) {
            for (PingPongCallback<TimestampResponse> cb : sequencedTimestampCallbacks.values()) {
                cb.error(e);
            }
            sequencedTimestampCallbacks.clear();
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
}
