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


package com.yahoo.omid.tso.persistence;


/**
 * an implementation of StateLogger using managed ledger of bookkeeper
 * managed ledger allows for the readers to consistently read from the same
 * cursur, even after failure
 */

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.AsyncCallback.CreateCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.yahoo.omid.tso.TSOHandler;
import com.yahoo.omid.tso.TSOServerConfig;
import com.yahoo.omid.tso.persistence.StateLogger;
import com.yahoo.omid.tso.persistence.LoggerAsyncCallback.LoggerInitCallback;
import com.yahoo.omid.tso.persistence.LoggerAsyncCallback.BuilderInitCallback;
import com.yahoo.omid.tso.persistence.LoggerAsyncCallback.AddRecordCallback;
import com.yahoo.omid.tso.persistence.LoggerConstants;
import com.yahoo.omid.tso.persistence.LoggerException.Code;

import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenLedgerCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.Position;

public class ManagedLedgerStateLogger implements StateLogger {
    private static final Log LOG = LogFactory.getLog(ManagedLedgerStateLogger.class);

    private ZooKeeper zk;
    private BookKeeper bk;
    private ManagedLedger managedLedger;

    /**
     * Flag to determine whether this logger is operating or not.
     */
    boolean enabled = false;

    /**
     * Constructor creates a zookeeper and a bookkeeper objects.
     */
    public ManagedLedgerStateLogger(ZooKeeper zk) {
        if(LOG.isDebugEnabled()){
            LOG.debug("Constructing Logger");
        }
        this.zk = zk; 
    }

    /**
     * Initializes this logger object to add records. Implements the initialize 
     * method of the StateLogger interface.
     * 
     * @param cb
     * @param ctx
     */
    @Override
    public void initialize(final LoggerInitCallback cb, Object ctx) throws LoggerException {
        TSOServerConfig config = TSOServerConfig.configFactory();

        /*
         * Create new ledger for adding records
         */
        try {
            bk = new BookKeeper(new ClientConfiguration(), zk);
        } catch (Exception e) {
            LOG.error("Exception while initializing bookkeeper", e);
            e.printStackTrace();
            throw new LoggerException.BKOpFailedException();  
        } 
        ManagedLedgerFactory factory;
        try {
            factory = new ManagedLedgerFactoryImpl(bk, zk);
        } catch (Exception e) {
            LOG.error("Exception while creating managed ledger bookkeeper", e);
            e.printStackTrace();
            throw new LoggerException.BKOpFailedException();  
        } 

        final String path = LoggerConstants.OMID_SEQUENCERLEDGER_ID_PATH;
        ManagedLedgerConfig mlconf = new ManagedLedgerConfig();
        mlconf.setEnsembleSize(config.getEnsembleSize());
        mlconf.setQuorumSize(config.getQuorumSize());
        mlconf.setDigestType(BookKeeper.DigestType.CRC32);
        factory.asyncOpen(path, new OpenLedgerCallback() {
            public void openLedgerComplete(ManagedLedger ledger, Object ctx) {
                LOG.warn("Successfully opened the managed ledger " + path);
                managedLedger = ledger;
                cb.loggerInitComplete(Code.OK, ManagedLedgerStateLogger.this, ctx);
                enabled = true;
            }
            public void openLedgerFailed(ManagedLedgerException exception, Object ctx) {
                LOG.error("Failed to open the managed ledger " + path, exception );
                exception.printStackTrace();
                cb.loggerInitComplete(Code.BKOPFAILED, ManagedLedgerStateLogger.this, ctx);
            }
        }, ctx);
    }

    /**
     * Adds a record to the log of operations. The record is a byte array.
     * 
     * @param record
     * @param cb
     * @param ctx
     */
    @Override
    public void addRecord(byte[] record, final AddRecordCallback cb, Object ctx) {
        if(LOG.isDebugEnabled()){
            LOG.debug("Adding record.");
        }

        if(!enabled){
            cb.addRecordComplete(Code.LOGGERDISABLED, ctx);
            return;
        }

        this.managedLedger.asyncAddEntry(record,
                new AddEntryCallback() {
                    public void addComplete(Position position, Object ctx) {
                        if(LOG.isDebugEnabled()){
                            LOG.info("Add to ledger complete: " + position);
                        }
                        cb.addRecordComplete(Code.OK, ctx);
                    }
                    public void addFailed(ManagedLedgerException exception, Object ctx) {
                        LOG.error("Asynchronous add entry failed: " + exception);
                        exception.printStackTrace();
                        cb.addRecordComplete(Code.ADDFAILED, ctx);
                    }
                }, ctx);
    }


    /**
     * Shuts down this logger.
     */
    public void shutdown() {
        enabled = false;
        try {
            try {
                if(zk.getState() == ZooKeeper.States.CONNECTED)
                    zk.delete(LoggerConstants.OMID_SEQUENCERLEDGER_ID_PATH, -1);
            } catch (Exception e) {
                LOG.warn("Exception while deleting lock znode", e);
            }
            if(this.managedLedger != null) managedLedger.close();
            if(this.bk != null) bk.close();
            if(this.zk != null) zk.close();
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while closing logger.", e);
        } catch (ManagedLedgerException e) {
            LOG.warn("Interrupted while closing managed ledger.", e);
        } catch (BKException e) {
            LOG.warn("Exception while closing BookKeeper object.", e);
        }
    }

}

