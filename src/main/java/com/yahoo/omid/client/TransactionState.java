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

import com.yahoo.omid.tso.RowKey;

//TODO: rename TransactionState to TxnStateRef
public class TransactionState {
    TxnState txnState;

    TransactionState(long ts, TSOClient tsoClient) {
        txnState = new TxnPartitionState(ts, tsoClient);
    }

    /**
     * The base class for txn state
     */
    abstract class TxnState {
        public abstract boolean isGlobal();
    }

    /**
     * The global state of the txn
     */
    class TxnGlobalState extends TxnState {
        private NavigableMap<KeyRange,TxnPartitionState> partitions;

        public TxnGlobalState() {
        }

        public TxnPartitionState getPartition(RowKey key) throws TransactionException {
            KeyRange keyRange = new KeyRange(key);
            Map.Entry<KeyRange,TxnPartitionState> entry = partitions.floorEntry(keyRange);
            if (entry == null || !entry.getKey().includes(key)) {
                throw new TransactionException("No partition is mapped to key " + key);
            } else {
                return entry.getValue();
            }
        }

        @Override
        public boolean isGlobal() {
            return true;
        }
    }

    /**
     * The state of a transaction for a partition of status oracle
     */
    class TxnPartitionState extends TxnState {
        private long startTimestamp;
        private long commitTimestamp;
        private Set<RowKeyFamily> writtenRows;
        private Set<RowKey> readRows;
        TSOClient tsoclient;

        TxnPartitionState() {
            startTimestamp = 0;
            commitTimestamp = 0;
            this.writtenRows = new HashSet<RowKeyFamily>();
            this.readRows = new HashSet<RowKey>();
        }

        TxnPartitionState(long startTimestamp, TSOClient client) {
            this();
            this.startTimestamp = startTimestamp;;
            this.commitTimestamp = 0;
            this.tsoclient = client;
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
            readRows.add(row);
        }

        @Override
        public String toString() {
            return "TxnPartitionState-" + Long.toHexString(startTimestamp);
        }

        @Override
        public boolean isGlobal() {
            return true;
        }
    }
}
