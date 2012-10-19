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

import java.util.HashSet;
import java.util.Set;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import com.yahoo.omid.tso.RowKey;
import com.yahoo.omid.IsolationLevel;

//TODO: rename TransactionState to TxnStateRef
/**
 * For MegaOmid, the new implementation of TransactionState is actually a pointer
 * to the real state (i.e., TxnState), which could be a partition-specific state
 * or a global state, depending on the transaction type.
 */
public class TransactionState {
    TxnState txnState;
    /**
     * We need a pointer to the manager to properly report violations over the 
     * partition range. The manager could use this info to decide on the next
     * transaction type/partition.
     */
    TransactionManager txnManager;

    /**
     * create a new local state for a particular partition
     */
    TransactionState(long ts, TSOClient tsoClient, KeyRange keyRange, 
            TransactionManager txnManager) {
        txnState = new TxnPartitionState(ts, tsoClient, keyRange);
        this.txnManager = txnManager;
    }

    /**
     * create a new global state for a global transaction
     * covering all partitions
     */
    TransactionState(long sequence, long[] vts, 
            TreeMap<KeyRange,TSOClient> sortedRangeClientMap,
            TransactionManager txnManager) {
        txnState = new TxnGlobalState(sequence, vts, sortedRangeClientMap);
        this.txnManager = txnManager;
    }

    /**
     * @return any of the covered partitions
     */
    public TxnPartitionState getPartition()
        throws TransactionException {
        return getPartition(null);
    }

    /**
     * @return the partition that contains the rowKey
     * If there is no such partition covered under this transaction,
     * an exception is thrown
     */
    public TxnPartitionState getPartition(RowKey rowKey)
        throws TransactionException {
        TxnPartitionState tps = txnState.getPartition(rowKey);
        if (tps == null) {
            txnManager.reportFailedPartitioning();
            throw new InvalidTxnPartitionException("Need to start a global transaction");
        }
        txnManager.reportLastUsedPartition(tps.keyRange);
        //System.out.println("LAST: [" + (txnState.isGlobal() ? 'G' : 'L') + "] (key=" + rowKey + ") " + tps.keyRange);
        return tps;
    }

    /**
     * The base class for txn state
     */
    static abstract class TxnState {
        public abstract boolean isGlobal();
        /**
         * @return the partition that contains the rowKey, 
         * or any partition if rowKey is null
         *
         * If there is no such partition covered under this transaction,
         * an exception is thrown
         */
        public abstract TxnPartitionState getPartition(final RowKey rowKey) 
            throws TransactionException;
    }

    /**
     * The global state of a global txn
     * This state covers multiple paritions
     */
    static class TxnGlobalState extends TxnState {
        /**
         * The state of each partition covered by this transaction is kept in the
         * following map. For example, commit timestamp.
         */
        private NavigableMap<KeyRange,TxnPartitionState> partitions;
        /**
         * The sequence accociated with the start timestamp.
         * The sequnce is unique per client
         */
        private long sequence;

        /**
         * The vector state timestamp of the global transaction
         * i.e., one timestamp for each parititon
         */
        long[] vts;

        public TxnGlobalState(long sequence, long[] vts, TreeMap<KeyRange,TSOClient> sortedRangeClientMap) {
            this.sequence = sequence;
            this.vts = vts;
            partitions = new TreeMap();
            int i = 0;
            for (Map.Entry<KeyRange,TSOClient> entry: sortedRangeClientMap.entrySet()) {
                TxnPartitionState pstate =
                    new TxnPartitionState(vts[i], entry.getValue(), entry.getKey());
                partitions.put(entry.getKey(), pstate);
                i++;
            }
        }

        public long getSequence() {
            return sequence;
        }

        public NavigableMap<KeyRange,TxnPartitionState> getPartitions() {
            return partitions;
        }

        @Override
        public TxnPartitionState getPartition(final RowKey key)
            throws TransactionException {
            Map.Entry<KeyRange,TxnPartitionState> entry;
            if (key == null) {
                //return any partition
                entry = partitions.firstEntry();
            } else {
                KeyRange keyRange = new KeyRange(key);
                entry = partitions.floorEntry(keyRange);
                if (entry == null || !entry.getKey().includes(key))
                    throw new TransactionException("No partition is mapped to key " + key);
            }
            return entry.getValue();
        }

        @Override
        public boolean isGlobal() {
            return true;
        }
    }

    /**
     * The state of a transaction for a partition of status oracle
     */
    static class TxnPartitionState extends TxnState {
        KeyRange keyRange; //the range covered by this partition
        private long startTimestamp;
        private long commitTimestamp;
        private Set<RowKeyFamily> writtenRows = new HashSet<RowKeyFamily>();
        private Set<RowKey> readRows = new HashSet<RowKey>();
        /**
         * The TSOClient associated with this partition
         */
        TSOClient tsoclient;

        TxnPartitionState(long startTimestamp, TSOClient client, KeyRange keyRange) {
            this.startTimestamp = startTimestamp;;
            this.commitTimestamp = 0;
            this.tsoclient = client;
            this.keyRange = keyRange;
        }

        @Override
        public TxnPartitionState getPartition(final RowKey key)
            throws TransactionException {
            if (key == null || keyRange.includes(key))
                return this;
            System.out.println("Wrong partition for key " + key);
            return null;//indicating that the current partition does not cover key
        }

        public boolean covers(RowKey rowKey) {
            return keyRange.includes(rowKey);
        }

        long getStartTimestamp() {
            return startTimestamp;
        }

        long getCommitTimestamp() {
            return commitTimestamp;
        }

        void setStartTimestamp(long startTimestamp) {
            this.startTimestamp = startTimestamp;
        }

        void setCommitTimestamp(long commitTimestamp) {
            this.commitTimestamp = commitTimestamp;
        }

        RowKeyFamily[] getWrittenRows() {
            return writtenRows.toArray(new RowKeyFamily[0]);
        }

        void addWrittenRow(RowKeyFamily row) {
            writtenRows.add(row);
        }

        RowKey[] getReadRows() {
            return readRows.toArray(new RowKey[0]);
        }

        void addReadRow(RowKey row) {
            //It is not necessary to keep track of read rows if we do not check for rw
            if (IsolationLevel.checkForReadWriteConflicts)
                readRows.add(row);
        }

        @Override
        public String toString() {
            return "TxnPartitionState-" + Long.toHexString(startTimestamp);
        }

        @Override
        public boolean isGlobal() {
            return false;
        }
    }
}
