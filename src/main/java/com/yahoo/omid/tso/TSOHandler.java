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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.Set;
import java.util.Iterator;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;

import com.yahoo.omid.tso.messages.Peerable;
import com.yahoo.omid.tso.messages.AbortRequest;
import com.yahoo.omid.tso.messages.PeerIdAnnoncement;
import com.yahoo.omid.tso.messages.AbortedTransactionReport;
import com.yahoo.omid.tso.messages.CommitQueryRequest;
import com.yahoo.omid.tso.messages.CommitQueryResponse;
import com.yahoo.omid.tso.messages.CommitRequest;
import com.yahoo.omid.tso.messages.MultiCommitRequest;
import com.yahoo.omid.tso.messages.CommitResponse;
import com.yahoo.omid.tso.messages.CommittedTransactionReport;
import com.yahoo.omid.tso.messages.FullAbortReport;
import com.yahoo.omid.tso.messages.EldestUpdate;
import com.yahoo.omid.tso.messages.ReincarnationReport;
import com.yahoo.omid.tso.messages.FailedElderReport;
import com.yahoo.omid.tso.messages.LargestDeletedTimestampReport;
import com.yahoo.omid.tso.messages.TimestampRequest;
import com.yahoo.omid.tso.messages.TimestampResponse;
import com.yahoo.omid.tso.messages.PrepareCommit;
import com.yahoo.omid.tso.messages.PrepareResponse;
import com.yahoo.omid.tso.persistence.LoggerAsyncCallback.AddRecordCallback;
import com.yahoo.omid.tso.persistence.LoggerException;
import com.yahoo.omid.tso.persistence.LoggerException.Code;
import com.yahoo.omid.tso.persistence.LoggerProtocol;
import com.yahoo.omid.IsolationLevel;
import com.yahoo.omid.client.TSOClient;
import java.util.Arrays;
import java.util.HashSet;
import com.yahoo.omid.sharedlog.*;


/**
 * ChannelHandler for the TSO Server
 * NOTE: Not thread-safe
 * @author maysam
 *
 */
public class TSOHandler extends SimpleChannelHandler {

    private static final Log LOG = LogFactory.getLog(TSOHandler.class);

    /**
     * Bytes monitor
     */
    public static int globaltxnCnt = 0;
    public static int txnCnt = 0;
    public static int abortCount = 0;
    public static int globalabortCount = 0;
    public static int outOfOrderCnt = 0;
    public static int hitCount = 0;
    public static long queries = 0;

    /**
     * Channel Group
     */
    private ChannelGroup channelGroup = null;
    private static ChannelGroup clientChannels = new DefaultChannelGroup("clients");

    /**
     * Timestamp Oracle
     */
    private TimestampOracle timestampOracle = null;

    /**
     * The wrapper for the shared state of TSO
     */
    private TSOState sharedState;

    /**
     * Constructor
     * @param channelGroup
     */
    public TSOHandler(ChannelGroup channelGroup, TSOState state) {
        this.channelGroup = channelGroup;
        this.timestampOracle = state.getSO();
        this.sharedState = state;
        System.out.println("TMAX: " + sharedState.largestDeletedTimestamp);
    }

    /**
     * If write of a message was not possible before, we can do it here
     */
    @Override
    public void channelInterestChanged(ChannelHandlerContext ctx,
            ChannelStateEvent e) {
    }

    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        channelGroup.add(ctx.getChannel());
    }

    /**
     * Handle receieved messages
     */
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        Object msg = e.getMessage();
        Channel channel = null;
        if (msg instanceof Peerable)
            channel = getPeerChannel((Peerable)msg);//could be null
        if (channel == null)//then the response is sent to the sender
            channel = ctx.getChannel();
        if (msg instanceof TimestampRequest) {
            handle((TimestampRequest) msg, channel);
            return;
        } else if (msg instanceof CommitRequest) {//it also handles MultiCommitRequest
            handle((CommitRequest) msg, channel);
            return;
        } else if (msg instanceof FullAbortReport) {
            handle((FullAbortReport) msg, channel);
            return;
        } else if (msg instanceof AbortRequest) {
            handle((AbortRequest) msg, channel);
            return;
        } else if (msg instanceof PeerIdAnnoncement) {
            handle((PeerIdAnnoncement) msg, channel);
            return;
        } else if (msg instanceof ReincarnationReport) {
            handle((ReincarnationReport) msg, channel);
            return;
        } else if (msg instanceof CommitQueryRequest) {
            handle((CommitQueryRequest) msg, channel);
            return;
        } else if (msg instanceof PrepareCommit) {
            handle((PrepareCommit) msg, channel);
            return;
        }
        LOG.error("Unknown message received at TSO: " + msg + " to " + channel);
    }

    /**
     * Handling an abort request
     * In the case of 2PC txns, the abort must be requested through the sequencer
     * via a MultiCommitRequest message
     * This is important to handle probable concurrent abort and commit requests 
     * for the same 2PC txn
     */
    public void handle(AbortRequest msg, Channel channel) {
        //TODO: Uncommited could mess it up if we call finished for a very old txn
        //synchronized (sharedState) {
            //sharedState.uncommited.finished(msg.startTimestamp);
        //}
        synchronized (sharedState) {
            abortCount++;
            sharedState.processAbort(msg.startTimestamp);
            queueHalfAbort(msg.startTimestamp);
        }
    }

    /**
     * Handle the PeerIdAnnoncement message
     */
    public void handle(PeerIdAnnoncement msg, Channel channel) {
        int peerId = msg.getPeerId();
        Channel currChannel = sharedState.peerToChannelMap.get(peerId);
        if (currChannel != null)
            LOG.error("Reseting the channel for peer " + peerId);
        System.out.println("set channel for " + peerId);
        sharedState.peerToChannelMap.put(peerId, channel);
    }

    public Channel getPeerChannel(Peerable msg) {
        Channel channel = null;
        //see if the peer of the communication is not the sender
        if (msg.peerIsSpecified()) {
            channel = sharedState.peerToChannelMap.get(msg.getPeerId());
            if (channel == null) {
                //TODO: handle it properly
                LOG.error("Unkonwn peer " + msg.getPeerId() + " msg: " + msg);
                LOG.error(sharedState.peerToChannelMap);
            }
        }
        return channel;
    }

    /**
     * Handle the TimestampRequest message
     */
    public void handle(TimestampRequest msg, Channel channel) {
        TimestampResponse response = null;
        synchronized (sharedState) {
            try {
                long timestamp;
                timestamp = timestampOracle.next(null);
                //if we do not want to keep track of the commit, simply set it finished
                if (!msg.trackProgress) {
                    sharedState.uncommited.finished(timestamp);
                    //consider the transaction as committed
                    if (msg.globalTxn)
                        globaltxnCnt++;
                    else
                        txnCnt++;
                }
                if (msg.isSequenced())
                    response = new TimestampResponse(timestamp, msg.getSequence());
                else
                    response = new TimestampResponse(timestamp);
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }
        }

        LogReader logReader;
        synchronized (sharedState.channelToReaderMap) {
            logReader = sharedState.channelToReaderMap.get(channel);
            if (logReader == null) {

                synchronized (sharedState) {
                    //send initializing messages to the client
                    channel.write(new CommittedTransactionReport(sharedState.latestStartTimestamp, sharedState.latestCommitTimestamp));
                    synchronized (sharedState.hashmap) {
                        for (Long halfAborted : sharedState.hashmap.halfAborted) {
                            channel.write(new AbortedTransactionReport(halfAborted));
                        }
                        for (Iterator<Elder> failedElders = sharedState.elders.failedEldersIterator(); failedElders.hasNext(); ) {
                            Elder fe = failedElders.next();
                            channel.write(new FailedElderReport(fe.getId(), fe.getCommitTimestamp()));
                        }
                    }
                    channel.write(new AbortedTransactionReport(sharedState.latestHalfAbortTimestamp));
                    channel.write(new FullAbortReport(sharedState.latestFullAbortTimestamp));
                    channel.write(new LargestDeletedTimestampReport(sharedState.largestDeletedTimestamp));

                    logReader = new LogReader(sharedState.sharedLog, sharedState.logPersister, sharedState.logPersister.getGlobalPointer());
                    sharedState.channelToReaderMap.put(channel, logReader);
                    LOG.warn("init reader for: " + channel);

                    channelGroup.add(channel);
                    clientChannels.add(channel);
                    LOG.warn("Channel connected: " + sharedState.channelToReaderMap.size());
                }
            }
        }
        try {
            ChannelBuffer tail = logReader.tail();
            if (tail != null)
                channel.write(tail);
        } catch (SharedLogLateFollowerException lateE) {
            //reset connection to the client
            channel.close();
            synchronized (sharedState.channelToReaderMap) {
                sharedState.channelToReaderMap.remove(channel);
            }
        } catch (SharedLogException sharedE) {
            //reset connection to the client
            channel.close();
            synchronized (sharedState.channelToReaderMap) {
                sharedState.channelToReaderMap.remove(channel);
            }
        }

        channel.write(response);
    }

    private boolean finish;

    /**
     * Handle the PrepareCommit message
     */
    private void handle(PrepareCommit msg, Channel channel) {
        PrepareResponse reply = new PrepareResponse(msg.startTimestamp);
        sortRows(msg.readRows, msg.writtenRows);
        reply.committed = prepareCommit(msg.startTimestamp, msg.readRows, msg.writtenRows);
        LockOp lockOp = LockOp.ownIt;
        if (!reply.committed) {
            sharedState.failedPrepared.add(msg.startTimestamp);
            lockOp = LockOp.unlock;//do not change the current owner if there is any
        }
        setLocks(msg.readRows, msg.writtenRows, lockOp, msg.startTimestamp);
        PrepareCommit prevValue = sharedState.prepareCommitHistory.put(msg.startTimestamp, msg);
        //System.out.println("sharedState.prepareCommitHistory.put " + msg.startTimestamp + " " + prevValue);
        assert(prevValue == null);
        channel.write(reply);
    }

    /**
     * Handle the CommitRequest message
     */
    private void handle(CommitRequest msg, Channel channel) {
        if (msg instanceof MultiCommitRequest) {
            //point to the related start timestamp in the vector timestamp
            ((MultiCommitRequest)msg).setSoId(sharedState.getId());
        }
        CommitResponse reply = new CommitResponse(msg.getStartTimestamp());
        sortRows(msg.readRows, msg.writtenRows);
        if (msg.prepared) {
            //System.out.println("sharedState.prepareCommitHistory.remove " + msg.getStartTimestamp());
            PrepareCommit prep = sharedState.prepareCommitHistory.remove(msg.getStartTimestamp());
            if (prep == null) {
                /**
                * prep could be null if the second phase of 2PC is already processed
                * receives redudant requests for the 2nd phase could be due to the 
                * concurrent activitiy of a watcher that suggests aborting a slow txn
                */
                LOG.warn("CommitRequest for non-pending transaction! " +
                        msg.getStartTimestamp() + "|" + msg + "|" + sharedState.prepareCommitHistory.size());
                //LOG.error(msg.getStartTimestamp() + "|" + msg + "|" + sharedState.prepareCommitHistory);
                return;
            }
            msg.readRows = prep.readRows;
            msg.writtenRows = prep.writtenRows;
            reply.committed = msg.successfulPrepared;
        }
        else
            reply.committed = prepareCommit(msg.getStartTimestamp(), msg.readRows, msg.writtenRows);
        try {
            if (reply.committed) {
                if (msg.writtenRows.length > 0) {
                    doCommit(msg, reply);
                }
                else {
                    reply.commitTimestamp = msg.getStartTimestamp();//default: Tc=Ts for read-only
                }
            }
            else {
                doAbort(msg, reply);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        //release the locks and send the response
        LockOp lockOp = LockOp.unlock;
        if (msg.prepared) {
            boolean wasFailed = sharedState.failedPrepared.remove(msg.getStartTimestamp());//just in case it was failed
            lockOp = wasFailed ? LockOp.unlock : LockOp.disownIt;//if it was failed, it was not owned by us
        }
        setLocks(msg.readRows, msg.writtenRows, lockOp, msg.getStartTimestamp());
        if (reply.committed) {
            if (msg.globalTxn)
                globaltxnCnt++;
            else
                txnCnt++;
        } else {
            if (msg.globalTxn)
                globalabortCount++;
            else
                abortCount++;
        }
        sendResponse(channel, reply);
    }

    /**
     * Sort the rows to avoid deadlock when do locking
     */
    private void sortRows(RowKey[] readRows, RowKey[] writtenRows) {
        if (IsolationLevel.checkForReadWriteConflicts) {
            for (RowKey r : readRows)
                r.index = (r.hashCode() & 0x7FFFFFFF) % TSOState.MAX_ITEMS;
            Arrays.sort(readRows);//for reads I just need atomic access and do not need to hold the locks
        }
        for (RowKey r : writtenRows)
            r.index = (r.hashCode() & 0x7FFFFFFF) % TSOState.MAX_ITEMS;
        Arrays.sort(writtenRows);//to avoid deadlocks
    }

    static final boolean LOCKIT = true;
    /**
     * Check for potential conflicts and lock the corresponding rows
     */
    private boolean prepareCommit(long startTimestamp, RowKey[] readRows, RowKey[] writtenRows) {
        boolean committed = true;//default
        //make sure it will not be aborted concurrently by a raise in Tmax on uncommited
        //if prepareCommit is successful, the transacion shall not be forced to abort
        //by a raise in Tmax, so finished should be called at this stage
        synchronized (sharedState) {
            sharedState.uncommited.finished(startTimestamp);
        }
        //0. check if it sould abort
        if (startTimestamp < timestampOracle.first()) {
            committed = false;
            LOG.warn("Aborting transaction after restarting TSO");
        } else if (startTimestamp < sharedState.largestDeletedTimestamp) {
            // Too old
            committed = false;//set as abort
            LOG.warn("Too old starttimestamp: ST "+ startTimestamp +" MAX " + sharedState.largestDeletedTimestamp);
        } else if (writtenRows.length > 0) {
            //1. check the read-write conflicts
            //for reads just need atomic access, but we need to hold locks since others 
            //might update them before we obtain the commit timestamp
            //2. readRows matters only if we checkForReadWriteConflicts, but if it is not set, readRows are empty anyway
            //always lock writes, since gonna update them anyway
            committed = setLocks(readRows, writtenRows, LockOp.lock, startTimestamp);
            if (IsolationLevel.checkForReadWriteConflicts)
                committed = checkForConflictsIn(readRows, startTimestamp, committed, true);
            if (IsolationLevel.checkForWriteWriteConflicts)
                committed = checkForConflictsIn(writtenRows, startTimestamp, committed, true);
        }
        return committed;
    }

    /**
     * The potential conflicting rows are locked and no conflict is found.
     * Do the commit and release the locks
     */
    private void doCommit(CommitRequest msg, CommitResponse reply) 
        throws IOException {
        //this variable allows avoid sync blocks by not accessing largestDeletedTimestamp
        long newmax = -1;
        long oldmax = -1;
        //2. commit
        reply.commitTimestamp = msg.getStartTimestamp();//default: Tc=Ts for read-only

        Set<Long> toAbort = null;
        //2.5 check the write-write conflicts to detect elders
        if (!IsolationLevel.checkForWriteWriteConflicts)
            checkForElders(reply, msg);
        //The following steps must be synchronized
        synchronized (sharedState) {
            newmax = oldmax = sharedState.largestDeletedTimestamp;
            //a) obtaining a commit timestamp
            reply.commitTimestamp = timestampOracle.next(null);
            //b) for the sake of efficiency do this, otherwise raise in Tmax causes perofrmance problems
            sharedState.uncommited.finished(reply.commitTimestamp);
            //c) commit the transaction
            //newmax = sharedState.hashmap.setCommitted(msg.startTimestamp, reply.commitTimestamp, newmax);
            newmax = sharedState.processCommit(msg.getStartTimestamp(), reply.commitTimestamp, newmax);
            if (reply.rowsWithWriteWriteConflict != null && reply.rowsWithWriteWriteConflict.size() > 0) {//if it is supposed to be reincarnated, also map Tc to Tc just in case of a future query.
                newmax = sharedState.hashmap.setCommitted(reply.commitTimestamp, reply.commitTimestamp, newmax);
            }
            //d) report the commit to the immdediate next txn
            queueCommit(msg.getStartTimestamp(), reply.commitTimestamp);
            //e) report eldest if it is changed by this commit
            reportEldestIfChanged(reply, msg);
            //f) report Tmax if it is changed
            if (newmax > oldmax) {//I caused a raise in Tmax
                sharedState.largestDeletedTimestamp = newmax;
                toAbort = sharedState.uncommited.raiseLargestDeletedTransaction(newmax);
                if (!toAbort.isEmpty())
                    LOG.warn("Slow transactions after raising max: " + toAbort);
                //TODO: toAbort should be added to half-aborted list
                for (Long id : toAbort)
                    queueHalfAbort(id);
                queueLargestIncrease(sharedState.largestDeletedTimestamp);
            }
        }
        //now do the rest out of sync block to allow more concurrency
        if(LOG.isDebugEnabled()){
            LOG.debug("Adding commit to WAL");
        }
        //TODO: should we be able to recover the writeset of failed elders?
        if (newmax > oldmax) {//I caused a raise in Tmax
            synchronized (sharedState.hashmap) {
                for (Long id : toAbort)
                    sharedState.hashmap.setHalfAborted(id);
            }
            if (!IsolationLevel.checkForWriteWriteConflicts) {
                Set<Elder> eldersToBeFailed = sharedState.elders.raiseLargestDeletedTransaction(newmax);
                if (eldersToBeFailed != null && !eldersToBeFailed.isEmpty()) {
                    LOG.warn("failed elder transactions after raising max: " + eldersToBeFailed + " from " + oldmax + " to " + newmax);
                    //report failedElders to the clients
                    for (Elder elder : eldersToBeFailed)
                        queueFailedElder(elder.getId(), elder.getCommitTimestamp());
                }
            }
        }
        for (RowKey r: msg.writtenRows)
            sharedState.hashmap.put(r.getRow(), r.getTable(), reply.commitTimestamp, r.hashCode());

    }

    private void doAbort(CommitRequest msg, CommitResponse reply)
        throws IOException {
        synchronized (sharedState.hashmap) {
            sharedState.processAbort(msg.getStartTimestamp());
        }
        queueHalfAbort(msg.getStartTimestamp());
    }

    enum LockOp {
        lock,
        unlock,
        ownIt,
        disownIt
    }

    /**
     * set the locks
     * a1 and a2 are sorted but they have duplicate keys
     * iterate over items in the sort order
     * careful not to lock/unlock the same index twice
     */
    private boolean setLocks(RowKey[] a1, RowKey[] a2, LockOp lockOp, long startTimestamp) {
        boolean result = true;
        int lastIndex = -1, index;
        int a1i = 0, a2i = 0;
        while (a1i < a1.length || a2i < a2.length) {
            if (a1i < a1.length && a1[a1i].index == lastIndex) {
                a1i++;
                continue;
            }
            if (a2i < a2.length && a2[a2i].index == lastIndex) {
                a2i++;
                continue;
            }
            if ((a1i < a1.length) && (a2i >= a2.length || a2[a2i].index > a1[a1i].index)) {
                index = a1[a1i].index;
                a1i++;
            } else {
                index = a2[a2i].index;
                a2i++;
            }
            switch (lockOp) {
                case lock:
                    boolean lockres = sharedState.hashmap.lock(index, startTimestamp);
                    result = result && lockres;
                    break;
                case unlock:
                    sharedState.hashmap.unlock(index);
                    break;
                case ownIt:
                    sharedState.hashmap.unlock(index, true);
                    break;
                case disownIt:
                    sharedState.hashmap.unlock(index, false);
                    break;
                default: System.exit(1);
            }
            lastIndex = index;
        }
        return result;
    }

    /**
     * send the response to the client
     */
    private void sendResponse(Channel channel, CommitResponse reply) {
        if (somethingIsLogged) {
            long writeIndex = sharedState.logWriter.getGlobalPointer();
            sharedState.outMsgs.add(new TSOState.ToBeLoggedMessage(
                        reply, channel, writeIndex));
            //maybe the logging was so fast the pending content is already logged
            //give it a check
            sharedState.letPersistedMessagesGo();
        } else {
            channel.write(reply);
        }
        somethingIsLogged = false;//reset it
        //The message will be sent, after the refered index in the log is persisted
    }

    boolean checkForConflictsIn(RowKey[] rows, long startTimestamp, boolean committed, boolean isAlreadyLocked) {
        if (!committed)//already aborted
            return committed;
        for (RowKey r: rows) {
            long value;
            if (isAlreadyLocked)
                value = sharedState.hashmap.get(r.getRow(), r.getTable(), r.hashCode());
            else//perform an atomic read that acquires the lock and releases it afterwards
                value = sharedState.hashmap.atomicget(r.getRow(), r.getTable(), r.hashCode(), r.index, startTimestamp);
            if (value != 0 && value > startTimestamp) {
                return false;//set as abort
            } else if (value == 0 && sharedState.largestDeletedTimestamp > startTimestamp) {
                //then it could have been committed after start timestamp but deleted by recycling
                LOG.warn("Old............... " + sharedState.largestDeletedTimestamp + " " + startTimestamp);
                return false;//set as abort
            } else if (value == -1) {//means that tmaxForConflictChecking > startTimestamp
                LOG.warn("Old....-1......... " + sharedState.largestDeletedTimestamp + " " + startTimestamp);
                return false;//set as abort
            }
        }
        return true;
    }

    //check for write-write conflicts
    void checkForElders(CommitResponse reply, CommitRequest msg) {
        for (RowKey r: msg.writtenRows) {
            long value;
            value = sharedState.hashmap.get(r.getRow(), r.getTable(), r.hashCode());
            if (value != 0 && value > msg.getStartTimestamp()) {
                aWWconflictDetected(reply, msg, r);
            } else if (value == 0 && sharedState.largestDeletedTimestamp > msg.getStartTimestamp()) {
                //then it could have been committed after start timestamp but deleted by recycling
                aWWconflictDetected(reply, msg, r);
            }
        }
    }

    void reportEldestIfChanged(CommitResponse reply, CommitRequest msg) 
        throws IOException{
        //2. add it to elders list
        if (reply.rowsWithWriteWriteConflict != null && reply.rowsWithWriteWriteConflict.size() > 0) {
            ArrayList<RowKey> rowsWithWriteWriteConflict = new ArrayList<RowKey>(reply.rowsWithWriteWriteConflict);
            sharedState.elders.addElder(msg.getStartTimestamp(), reply.commitTimestamp, rowsWithWriteWriteConflict);
            if (sharedState.elders.isEldestChangedSinceLastProbe()) {
                LOG.warn("eldest is changed: " + msg.getStartTimestamp());
                queueEldestUpdate(sharedState.elders.getEldest());
            }
            else
                LOG.warn("eldest " + sharedState.elders.getEldest() + " isnt changed by ww " + msg.getStartTimestamp() );
        }
    }

    //A write-write conflict is detected and the proper action is taken here
    void aWWconflictDetected(CommitResponse reply, CommitRequest msg, RowKey wwRow) {
        //Since we abort only for read-write conflicts, here we just keep track of elders (transactions with ww conflict) and tell them to reincarnate themselves by reinserting the items with ww conflict
        //1. add it to the reply to the lients
        if (reply.rowsWithWriteWriteConflict == null)
            //I do not know the size, so I create the longest needed
            reply.rowsWithWriteWriteConflict = new ArrayList<RowKey>(msg.writtenRows.length);
        reply.rowsWithWriteWriteConflict.add(wwRow);
    }

    /**
     * Handle the CommitQueryRequest message
     */
    public void handle(CommitQueryRequest msg, Channel channel) {
        CommitQueryResponse reply = new CommitQueryResponse(msg.startTimestamp);
        reply.queryTimestamp = msg.queryTimestamp;
        synchronized (sharedState) {
            queries++;
            long value;
            value = sharedState.hashmap.getCommittedTimestamp(msg.queryTimestamp);
            if (value != 0) { //it exists
                reply.commitTimestamp = value;
                reply.committed = value < msg.startTimestamp;//set as abort
            }
            else if (sharedState.largestDeletedTimestamp < msg.queryTimestamp) 
                reply.committed = false;
            else if (sharedState.hashmap.isHalfAborted(msg.queryTimestamp))
                reply.committed = false;
            else 
                reply.retry = true;
            //retry is show that we cannot distinguish two cases
            //1. Tc < Tmax
            //2. Ts < Tmax && aborted && Cleanedup is sent after we read the value but is received before this query is processed.

            channel.write(reply);
            // We send the message directly. If after a failure the state is inconsistent we'll detect it
        }
    }

  //private void flushTheBatch() {
  //    for (ChannelandMessage cam : theBatch) {
  //        cam.channel.write(cam.msg);
  //    }
  //}

  //private class FlushThread implements Runnable {
  //    @Override
  //    public void run() {
  //        if (finish) {
  //            return;
  //        }
  //        if (sharedState.nextBatch.size() > 0) {
  //            synchronized (sharedState) {
  //                if (sharedState.nextBatch.size() > 0) {
  //                    if(LOG.isDebugEnabled()){
  //                        LOG.debug("Flushing log batch.");
  //                    }
  //                    flushTheBatch();
  //                }
  //            }
  //        }
  //        flushFuture = executor.schedule(flushThread, TSOState.FLUSH_TIMEOUT, TimeUnit.MILLISECONDS);
  //    }
  //}

    /**
     * Used to check if anything is logged during the process of this message
     * e.g., if nothing is logged for a readonly message, do not have to wait
     * for persistence of log
     */
    boolean somethingIsLogged = false;
    /**
     * This is a temp buffer used pass the encoded message to the log
     */
    ChannelBuffer tempLogBuffer = ChannelBuffers.buffer(50);
    //30 is overestimation of tr size
    private void logIt(ChannelBuffer buf) {
        somethingIsLogged = true;
        try {
            //System.out.println("logIt");
            sharedState.logWriter.append(buf);
        } catch (SharedLogException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void queueCommit(long startTimestamp, long commitTimestamp) {
        tempLogBuffer.clear();
        sharedState.sharedMessageBuffer.writeCommit(tempLogBuffer, startTimestamp, commitTimestamp);
        //append is synchronized
        logIt(tempLogBuffer);
    }

    private void queueHalfAbort(long startTimestamp) {
        tempLogBuffer.clear();
        sharedState.sharedMessageBuffer.writeHalfAbort(tempLogBuffer, startTimestamp);
        //append is synchronized
        logIt(tempLogBuffer);
    }

    private void queueEldestUpdate(Elder eldest) {
        tempLogBuffer.clear();
        long startTimestamp = eldest == null ? -1 : eldest.getId();
        sharedState.sharedMessageBuffer.writeEldest(tempLogBuffer, startTimestamp);
        //append is synchronized
        logIt(tempLogBuffer);
    }

    private void queueReincarnatdElder(long startTimestamp) {
        tempLogBuffer.clear();
        sharedState.sharedMessageBuffer.writeReincarnatedElder(tempLogBuffer, startTimestamp);
        //append is synchronized
        logIt(tempLogBuffer);
    }

    private void queueFailedElder(long startTimestamp, long commitTimestamp) {
        tempLogBuffer.clear();
        sharedState.sharedMessageBuffer.writeFailedElder(tempLogBuffer, startTimestamp, commitTimestamp);
        //append is synchronized
        logIt(tempLogBuffer);
    }

    private void queueFullAbort(long startTimestamp) {
        tempLogBuffer.clear();
        sharedState.sharedMessageBuffer.writeFullAbort(tempLogBuffer, startTimestamp);
        //append is synchronized
        logIt(tempLogBuffer);
    }

    private void queueLargestIncrease(long largestTimestamp) {
        tempLogBuffer.clear();
        sharedState.sharedMessageBuffer.writeLargestIncrease(tempLogBuffer, largestTimestamp);
        //append is synchronized
        logIt(tempLogBuffer);
    }

    /**
     * Handle the ReincarnationReport message
     */
    public void handle(ReincarnationReport msg, Channel channel) {
        synchronized (sharedState) {
            LOG.warn("reincarnated: " + msg.startTimestamp);
            boolean itWasFailed = sharedState.elders.reincarnateElder(msg.startTimestamp);
            if (itWasFailed) {
                LOG.warn("a failed elder is reincarnated: " + msg.startTimestamp);
                //tell the clients that the failed elder is reincarnated
                queueReincarnatdElder(msg.startTimestamp);
            }
            if (sharedState.elders.isEldestChangedSinceLastProbe()) {
                LOG.warn("eldest is changed: " + msg.startTimestamp);
                queueEldestUpdate(sharedState.elders.getEldest());
            }
            else
                LOG.warn("eldest " + sharedState.elders.getEldest() + " isnt changed by reincarnated " + msg.startTimestamp );
        }
    }

    /**
     * Handle the FullAbortReport message
     */
    public void handle(FullAbortReport msg, Channel chennel) {
        synchronized (sharedState) {
            sharedState.processFullAbort(msg.startTimestamp);
        }
        queueFullAbort(msg.startTimestamp);
    }

    /*
     * Wrapper for Channel and Message
     */
    public static class ChannelandMessage {
        Channel channel;
        TSOMessage msg;
        ChannelandMessage(Channel c, TSOMessage m) {
            channel = c;
            msg = m;
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
        LOG.warn("TSOHandler: Unexpected exception from downstream.", e.getCause());
        e.getCause().printStackTrace();
        Channels.close(e.getChannel());
    }

    public void stop() {
        finish = true;
    }


}

