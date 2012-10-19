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

import java.util.Set;
import java.util.HashSet;
import java.util.Queue;
import java.util.NoSuchElementException;
import java.util.LinkedList;

/**
 * A set of aborted transactions that could be accessed by concurrent transactions
 * If a transactin Ta is in the aborted list at the time transaction Tr starts, 
 * it must stay there till Tr commits/aborts. 
 * This is necessary to avoid the corner cases that Tr reads a value that 
 * had been written by Ta but since Ta is not in aborted list anymore, 
 * Tr takes the value as committed. Refer to HotDep for furthur details.
 * The trick is to delay removal of aborts until the transactions that 
 * started before the remove invocation are finished.
 *
 * @author maysam
 */
public class SharedAbortedSet {
    protected Set<Long> aborted = new HashSet<Long>(1000);
    protected DeleteEfficientQueue<Txn> queuedFullAborted = new DeleteEfficientQueue();

    public synchronized void clear() {
        aborted.clear();
        queuedFullAborted.clear();
    }

    public synchronized boolean contains(Long txn) {
        return aborted.contains(txn);
    }

    //remove a txn from the aborted set
    public synchronized void remove(Long txn) {
        if (queuedFullAborted.peek() == null)//no transaction started
            aborted.remove(txn);
        else //delay the removal of the aborted txn
            queuedFullAborted.offer(new FullAbortedTxn(txn));
    }

    //add a txn to the aborted set
    public synchronized void add(Long txn) {
        aborted.add(txn);
    }

    public synchronized void aTxnStarted(Long txn) {
        queuedFullAborted.offer(new StartedTxn(txn));
    }

    public synchronized void aTxnFinished(Long txn) {
        queuedFullAborted.delete(new StartedTxn(txn));
        //after a transaction is finished, apply all full aborts that were receieved after the transaction start and do not have to be delayed due to other started txns
        Txn top = queuedFullAborted.peek();
        while (top instanceof FullAbortedTxn) {
            aborted.remove(top.Ts);
            queuedFullAborted.poll();
            top = queuedFullAborted.peek();
        }
    }

    byte StartedTxnType = 0;
    byte FullAbortedTxnType = 1;
    abstract class Txn {
        public Long Ts;
        protected byte type;
        public Txn(Long Ts) {
            this.Ts = Ts;
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof Txn && o != null) {
                Txn txn = (Txn)o;
                return txn.Ts == Ts && txn.type == type;
            }
            return false;
        }

        Integer hash = null;
        @Override
        public int hashCode() {
            if (hash == null)
                hash = Ts.hashCode();
            return hash;
        }
    }

    class StartedTxn extends Txn {
        public StartedTxn(Long Ts) {
            super(Ts);
            type = StartedTxnType;
        }
    }

    class FullAbortedTxn extends Txn {
        public FullAbortedTxn(Long Ts) {
            super(Ts);
            type = FullAbortedTxnType;
        }
    }
}

