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
public class TransactionState {
    TxnState txnState;
    TransactionManager txnManager;

    TransactionState(long ts, TSOClient tsoClient, KeyRange keyRange, 
            TransactionManager txnManager) {
        txnState = new TxnPartitionState(ts, tsoClient, keyRange);
        this.txnManager = txnManager;
    }

    TransactionState(long sequence, long[] vts, 
            TreeMap<KeyRange,TSOClient> sortedRangeClientMap,
            TransactionManager txnManager) {
        txnState = new TxnGlobalState(sequence, vts, sortedRangeClientMap);
        this.txnManager = txnManager;
    }

    public TxnPartitionState getPartition()
        throws TransactionException {
        return getPartition(null);
    }

    public TxnPartitionState getPartition(RowKey rowKey)
        throws TransactionException {
        TxnPartitionState tps = txnState.getPartition(rowKey);
        if (tps == null) {
            txnManager.reportFailedPartitioning();
            throw new InvalidTxnPartitionException("Need to start a global transaction");
        }
        txnManager.reportLastUsedPartition(tps.keyRange, tps.tsoclient);
        //System.out.println("LAST: [" + (txnState.isGlobal() ? 'G' : 'L') + "] (key=" + rowKey + ") " + tps.keyRange + " " + tps.tsoclient);
        return tps;
    }

    /**
     * The base class for txn state
     */
    static abstract class TxnState {
        public abstract boolean isGlobal();
        public abstract TxnPartitionState getPartition(final RowKey rowKey) 
            throws TransactionException;
    }

    /**
     * The global state of the txn
     */
    static class TxnGlobalState extends TxnState {
        private NavigableMap<KeyRange,TxnPartitionState> partitions;
        private long sequence;
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

        public TxnPartitionState getPartition(final RowKey key)
            throws TransactionException {
            Map.Entry<KeyRange,TxnPartitionState> entry;
            if (key == null) {
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
        TSOClient tsoclient;

        TxnPartitionState(long startTimestamp, TSOClient client, KeyRange keyRange) {
            this.startTimestamp = startTimestamp;;
            this.commitTimestamp = 0;
            this.tsoclient = client;
            this.keyRange = keyRange;
        }

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
