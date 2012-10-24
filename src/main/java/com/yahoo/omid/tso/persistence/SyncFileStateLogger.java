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
 * an implementation of StateLogger using files (over nfs)
 */

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

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

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import org.apache.zookeeper.ZooKeeper;

/**
 * A very simple logger backed by a file perhaps over nfs
 * Note: it is synchrounous
 */
public class SyncFileStateLogger implements StateLogger {
    private static final Log LOG = LogFactory.getLog(SyncFileStateLogger.class);

    private FileOutputStream file;

    /**
     * Flag to determine whether this logger is operating or not.
     */
    boolean enabled = false;

    /**
     * Constructor creates a zookeeper and a bookkeeper objects.
     */
    public SyncFileStateLogger(ZooKeeper zk) {
        if(LOG.isDebugEnabled()){
            LOG.debug("Constructing Logger");
        }
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
        initialize(cb, ctx, LoggerConstants.OMID_SEQUENCERLEDGER_ID_PATH);
    }

    public void initialize(final LoggerInitCallback cb, Object ctx, String filename) throws LoggerException {
        final String path = LoggerConstants.OMID_NFS_PATH + "/" + filename;
        final boolean APPEND = true;
        File fileRef = new File(path);
        try {
            //TODO: append must be true not to delete the WAL
            //but temporarily I set it to false since currently
            //the WAL readers do not persistently store the last read cursor
            file = new FileOutputStream(fileRef, APPEND==false);
            LOG.warn("Successfully opened the file " + fileRef);
            cb.loggerInitComplete(Code.OK, this, ctx);
            enabled = true;
        } catch (FileNotFoundException e) {
            LOG.error("File " + path + " not found", e);
            e.printStackTrace();
            cb.loggerInitComplete(Code.BKOPFAILED, this, ctx);
        }
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

        try {
            file.write(record);
            file.flush();
            if(LOG.isDebugEnabled()){
                LOG.info("Append to file complete: ");
            }
            cb.addRecordComplete(Code.OK, ctx);
        } catch (IOException e) {
            LOG.error("Error in writing to file", e);
            e.printStackTrace();
            System.exit(1);
            //TODO: handle properly
        }
    }


    /**
     * Shuts down this logger.
     */
    public void shutdown() {
        enabled = false;
        try {
            file.close();
        } catch (IOException e) {
            LOG.error("Interrupted while closing file.", e);
            e.printStackTrace();
            System.exit(1);
            //TODO: handle properly
        }
    }

}


