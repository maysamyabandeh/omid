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

import com.yahoo.omid.tso.RowKey;
import com.yahoo.omid.tso.messages.CommitRequest;
import com.yahoo.omid.tso.messages.CommitResponse;
import com.yahoo.omid.tso.messages.TimestampResponse;
import com.yahoo.omid.Statistics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.HTable;

/**
 * Provides the methods necessary to create and commit transactions.
 * 
 * @see TransactionalTable
 *
 */
public class TransactionManager {
    private static final Log LOG = LogFactory.getLog(TSOClient.class);

    static TSOClient tsoclient = null;
    private static Object lock = new Object();
    private Configuration conf;
    private HashMap<byte[], HTable> tableCache;

    public TransactionManager(Configuration conf) throws TransactionException, IOException {
        this.conf = conf;
        java.util.Iterator<Map.Entry<String,String>> confIterator = conf.iterator();
        java.util.Properties prop = new java.util.Properties();
        while (confIterator.hasNext()) {
            Map.Entry<String,String> next = confIterator.next();
            prop.setProperty(next.getKey(), next.getValue());
        }
        synchronized (lock) {
            if (tsoclient == null) {
                tsoclient = new TSOClient(prop);
            }
        }
        tableCache = new HashMap<byte[], HTable>();
    }

    /**
     * Starts a new transaction.
     * 
     * This method returns an opaque {@link TransactionState} object, used by {@link TransactionalTable}'s methods
     * for performing operations on a given transaction.
     * 
     * @return Opaque object which identifies one transaction.
     * @throws TransactionException
     */
    public TransactionState beginTransaction() throws TransactionException {
        PingPongCallback<TimestampResponse> cb = new PingPongCallback<TimestampResponse>();
        try {
            tsoclient.getNewTimestamp(cb);
            cb.await();
        } catch (Exception e) {
            throw new TransactionException("Could not get new timestamp", e);
        }
        if (cb.getException() != null) {
            throw new TransactionException("Error retrieving timestamp", cb.getException());
        }

        TimestampResponse pong = cb.getPong();
        tsoclient.aborted.aTxnStarted(pong.timestamp);
        return new TransactionState(pong.timestamp, tsoclient);
    }

    /**
     * Commits a transaction. If the transaction is aborted it automatically rollbacks the changes and
     * throws a {@link CommitUnsuccessfulException}.  
     * 
     * @param transactionState Object identifying the transaction to be committed.
     * @throws CommitUnsuccessfulException
     * @throws TransactionException
     */
    public void tryCommit(TransactionState transactionState)
        throws CommitUnsuccessfulException, TransactionException {
        Statistics.fullReport(Statistics.Tag.COMMIT, 1);
        if (LOG.isTraceEnabled()) {
            LOG.trace("tryCommit " + transactionState.getStartTimestamp());
        }
        PingPongCallback<CommitResponse> cb = new PingPongCallback<CommitResponse>();
        try {
            CommitRequest msg = new CommitRequest(transactionState.getStartTimestamp(),
                    transactionState.getWrittenRows(),
                    transactionState.getReadRows());
            tsoclient.commit(transactionState.getStartTimestamp(), msg, cb);
            cb.await();
        } catch (Exception e) {
            throw new TransactionException("Could not commit", e);
        }
        if (cb.getException() != null) {
            throw new TransactionException("Error committing", cb.getException());
        }      

        CommitResponse pong = cb.getPong();
        if (LOG.isTraceEnabled()) {
            LOG.trace("doneCommit " + transactionState.getStartTimestamp() +
                    " TS_c: " + pong.commitTimestamp +
                    " Success: " + pong.committed);
        }

        tsoclient.aborted.aTxnFinished(transactionState.getStartTimestamp());

        if (!pong.committed) {
            cleanup(transactionState);
            throw new CommitUnsuccessfulException();
        }
        transactionState.setCommitTimestamp(pong.commitTimestamp);
        if (pong.isElder()) {
            reincarnate(transactionState, pong.rowsWithWriteWriteConflict);
            try {
                transactionState.tsoclient.completeReincarnation(transactionState.getStartTimestamp(), PingCallback.DUMMY);
            } catch (IOException e) {
                LOG.error("Couldn't send reincarnation report", e);
            }
        }
        Statistics.println();
    }

    /**
     * Aborts a transaction and automatically rollbacks the changes.
     * 
     * @param transactionState Object identifying the transaction to be committed.
     * @throws TransactionException
     */
    public void abort(TransactionState transactionState) throws TransactionException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("abort " + transactionState.getStartTimestamp());
        }
        try {
            tsoclient.abort(transactionState.getStartTimestamp());
        } catch (Exception e) {
            throw new TransactionException("Could not abort", e);
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace("doneAbort " + transactionState.getStartTimestamp());
        }

        tsoclient.aborted.aTxnFinished(transactionState.getStartTimestamp());

        // Make sure its commit timestamp is 0, so the cleanup does the right job
        transactionState.setCommitTimestamp(0);
        cleanup(transactionState);
    }

    private void reincarnate(final TransactionState transactionState, ArrayList<RowKey> rowsWithWriteWriteConflict)
        throws TransactionException {
        Statistics.fullReport(Statistics.Tag.REINCARNATION, 1);
        Map<byte[], List<Put>> putBatches = new HashMap<byte[], List<Put>>();
        for (final RowKeyFamily rowkey : transactionState.getWrittenRows()) {
            //TODO: do it only for rowsWithWriteWriteConflict
            List<Put> batch = putBatches.get(rowkey.getTable());
            if (batch == null) {
                batch = new ArrayList<Put>();
                putBatches.put(rowkey.getTable(), batch);
            }
            Put put = new Put(rowkey.getRow(), transactionState.getCommitTimestamp());
            for (Entry<byte[], List<KeyValue>> entry : rowkey.getFamilies().entrySet())
                for (KeyValue kv : entry.getValue())
                    try {
                        put.add(new KeyValue(kv.getRow(), kv.getFamily(), kv.getQualifier(), transactionState.getCommitTimestamp(), kv.getValue()));
                    } catch (IOException ioe) {
                        throw new TransactionException("Could not add put operation in reincarnation " + entry.getKey(), ioe);
                    }
            batch.add(put);
        }
        for (final Entry<byte[], List<Put>> entry : putBatches.entrySet()) {
            try {
                HTable table = tableCache.get(entry.getKey());
                if (table == null) {
                    table = new HTable(conf, entry.getKey());
                    tableCache.put(entry.getKey(), table);
                }
                table.put(entry.getValue());
            } catch (IOException ioe) {
                throw new TransactionException("Could not reincarnate for table " + entry.getKey(), ioe);
            }
        }
    }

    private void cleanup(final TransactionState transactionState)
        throws TransactionException {
        Map<byte[], List<Delete>> deleteBatches = new HashMap<byte[], List<Delete>>();
        for (final RowKeyFamily rowkey : transactionState.getWrittenRows()) {
            List<Delete> batch = deleteBatches.get(rowkey.getTable());
            if (batch == null) {
                batch = new ArrayList<Delete>();
                deleteBatches.put(rowkey.getTable(), batch);
            }
            Delete delete = new Delete(rowkey.getRow());
            for (Entry<byte[], List<KeyValue>> entry : rowkey.getFamilies().entrySet()) {
                for (KeyValue kv : entry.getValue()) {
                    delete.deleteColumn(entry.getKey(), kv.getQualifier(), transactionState.getStartTimestamp());
                }
            }
            batch.add(delete);
        }
        for (final Entry<byte[], List<Delete>> entry : deleteBatches.entrySet()) {
            try {
                HTable table = tableCache.get(entry.getKey());
                if (table == null) {
                    table = new HTable(conf, entry.getKey());
                    tableCache.put(entry.getKey(), table);
                }
                table.delete(entry.getValue());
            } catch (IOException ioe) {
                throw new TransactionException("Could not clean up for table " + entry.getKey(), ioe);
            }
        }
        try {
            tsoclient.completeAbort(transactionState.getStartTimestamp(), PingCallback.DUMMY );
        } catch (IOException ioe) {
            throw new TransactionException("Could not notify TSO about cleanup completion for transaction " +
                    transactionState.getStartTimestamp(), ioe);
        }
    }
}
