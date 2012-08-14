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
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import java.util.Collections;

import com.yahoo.omid.tso.persistence.LoggerException.Code;
import com.yahoo.omid.tso.persistence.BookKeeperStateBuilder;
import com.yahoo.omid.tso.persistence.StateLogger;
import com.yahoo.omid.tso.persistence.LoggerAsyncCallback.AddRecordCallback;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import org.jboss.netty.channel.Channel;
import com.yahoo.omid.tso.TSOSharedMessageBuffer.ReadingBuffer;
import com.yahoo.omid.tso.messages.PrepareCommit;
import com.yahoo.omid.client.TSOClient;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.Iterator;
import java.util.Properties;
import java.io.IOException;
import com.yahoo.omid.tso.messages.MultiCommitRequest;

/**
 * The wrapper for different states of TSO
 * This state is shared between handlers
 * @author maysam
 */
public class TSOState {
    private static final Log LOG = LogFactory.getLog(TSOState.class);
    
    /**
     * The mapping between the client channels and their correspondign pointer on
     * the shared message buffer
     */
    public Map<Channel, ReadingBuffer> messageBuffersMap = new HashMap<Channel, ReadingBuffer>();

    /**
     * The mapping between client Ids to their corresponding channel
     */
    public Map<Integer, Channel> peerToChannelMap = new ConcurrentHashMap<Integer, Channel>();

    /**
     * Mainatains a mapping between the transaction and its PrepareCommit
     * which contains the rows read and written by the transaction
     */
    //TODO: how about creating N maps and attach them to channels. This avoids concurrency inefficiency
    public ConcurrentMap<Long, PrepareCommit> prepareCommitHistory = new ConcurrentHashMap(10000);
    //TODO: estimate a proper capacity


    
   /**
    * The maximum entries kept in TSO
    */
   // static final public int MAX_ITEMS = 10000000;
   // static final public int MAX_ITEMS = 4000000;
   static public int MAX_ITEMS = 100;
   static {
      try {
         MAX_ITEMS = Integer.valueOf(System.getProperty("omid.maxItems"));
      } catch (Exception e) {
         // ignore, usedefault
      }
   };

   static public int MAX_COMMITS = 100;
   static {
      try {
         MAX_COMMITS = Integer.valueOf(System.getProperty("omid.maxCommits"));
      } catch (Exception e) {
         // ignore, usedefault
      }
   };

   static public int MONITOR_INTERVAL = 30;//in seconds
   static public int FLUSH_TIMEOUT = 5;
   static {
      try {
         FLUSH_TIMEOUT = Integer.valueOf(System.getProperty("omid.flushTimeout"));
      } catch (Exception e) {
         // ignore, usedefault
      }
   };

   /**
    * Hash map load factor
    */
   static final public float LOAD_FACTOR = 0.5f;

   /**
    * If requested are sequened, remember the last one
    */
   public long lastServicedSequence = -1;
   
   /**
    * Object that implements the logic to log records
    * for recoverability
    */
   
   StateLogger logger;

   public StateLogger getLogger(){
       return logger;
   }
   
   public void setLogger(StateLogger logger){
       this.logger = logger;
   }
   
   /**
    * Only timestamp oracle instance in the system.
    */
   private TimestampOracle timestampOracle;
   
   protected TimestampOracle getSO(){
       return timestampOracle;
   }
   
   /**
    * Largest Deleted Timestamp
    */
   public long largestDeletedTimestamp = 0;

   public long latestCommitTimestamp = 0;
   public long latestStartTimestamp = 0;
   public long latestHalfAbortTimestamp = 0;
   public long latestFullAbortTimestamp = 0;
   
   public TSOSharedMessageBuffer sharedMessageBuffer = new TSOSharedMessageBuffer(this);

   /**
    * The hash map to to keep track of recently committed rows
    * each bucket is about 20 byte, so the initial capacity is 20MB
    */
   public CommitHashMap hashmap = new CommitHashMap(MAX_ITEMS, LOAD_FACTOR);

   public Uncommited uncommited;

   // The list of the elders: the committed transactions with write write conflicts
   public Elders elders;

   public Set<Long> failedPrepared = Collections.synchronizedSet(new HashSet<Long>(100));

   int id; //the id of the so
   public int getId() {
       return id;
   }
   public void setId(int id) {
       this.id = id;
   }

   /**
    * Process commit request.
    * 
    * @param startTimestamp
    */
   protected long processCommit(long startTimestamp, long commitTimestamp){
      return processCommit(startTimestamp, commitTimestamp, largestDeletedTimestamp);
   }
   protected long processCommit(long startTimestamp, long commitTimestamp, long newmax){
       newmax = hashmap.setCommitted(startTimestamp, commitTimestamp, newmax);
       return newmax;
   }
   
   /**
    * Process largest deleted timestamp.
    * 
    * @param largestDeletedTimestamp
    */
   protected synchronized void processLargestDeletedTimestamp(long largestDeletedTimestamp){
       this.largestDeletedTimestamp = Math.max(largestDeletedTimestamp, this.largestDeletedTimestamp);
   }
   
   /**
    * Process abort request.
    * 
    * @param startTimestamp
    */
   protected void processAbort(long startTimestamp){
       hashmap.setHalfAborted(startTimestamp);
   }
   
   /**
    * Process full abort report.
    * 
    * @param startTimestamp
    */
   protected void processFullAbort(long startTimestamp){
       hashmap.setFullAborted(startTimestamp);
   }

   /**
    * If logger is disabled, then this call is a noop.
    * 
    * @param record
    * @param cb
    * @param ctx
    */
   public void addRecord(byte[] record, final AddRecordCallback cb, Object ctx) {
       if(logger != null){
           logger.addRecord(record, cb, ctx);
       } else{
           cb.addRecordComplete(Code.OK, ctx);
       }
   }
   
   /**
    * Closes this state object.
    */
   void stop(){
       if(logger != null){
           logger.shutdown();
       }
   }
   
   /*
    * WAL related pointers
    */
   public static int BATCH_SIZE = 0;//in bytes
   public ByteArrayOutputStream baos = new ByteArrayOutputStream();
   public DataOutputStream toWAL = new DataOutputStream(baos);
   public List<TSOHandler.ChannelandMessage> nextBatch = new ArrayList<TSOHandler.ChannelandMessage>();
   
   public TSOState(StateLogger logger, TimestampOracle timestampOracle) {
       this.timestampOracle = timestampOracle;
       this.largestDeletedTimestamp = this.timestampOracle.get();
       this.uncommited = new Uncommited(largestDeletedTimestamp);
       this.elders = new Elders();
       this.logger = logger;
       startsLockMonitor();
   }
   
   public TSOState(TimestampOracle timestampOracle) {
       this.timestampOracle = timestampOracle;
       this.largestDeletedTimestamp = timestampOracle.get();
       this.uncommited = new Uncommited(largestDeletedTimestamp);
       this.elders = new Elders();
       this.logger = null;
       startsLockMonitor();
    }

    /**
     * The interface to the sequencer
     * It should be created after the sequencer is up
     */
    TSOClient sequencerClient = null;

    /**
     * The locks used to synchronize access to the corresponding objects
     */
    Object sharedMsgBufLock = new Object();
    Object callbackLock = new Object();

    void startsLockMonitor() {
        LockMonitor lockMonitor = new LockMonitor();
        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(lockMonitor, MONITOR_INTERVAL, TSOState.MONITOR_INTERVAL, TimeUnit.SECONDS);
    }

    void initSequencerClient(Properties sequencerConf) {
        try {
            sequencerClient = new TSOClient(sequencerConf);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * This class monitors the unreleased locks and 
     * do proper actions accordingly
     */
    private class LockMonitor implements Runnable {
        @Override
        public void run() {
            try {
                Iterator it = prepareCommitHistory.entrySet().iterator();
                long time = System.currentTimeMillis();
                while (it.hasNext()) {
                    Map.Entry pairs = (Map.Entry)it.next();
                    PrepareCommit prep = ((PrepareCommit)pairs.getValue());
                    if (prep.isAlreadyVisitedByLockMonitor)
                        suggestAbort(prep);
                    else prep.isAlreadyVisitedByLockMonitor = true;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Suggest aborting a transaction
     */
    void suggestAbort(PrepareCommit prep) {
        MultiCommitRequest mcr = new MultiCommitRequest(prep);
        mcr.successfulPrepared = false;
        LOG.warn("Suggesting abort: " + mcr + " to " + sequencerClient);
        try {
            sequencerClient.forward(mcr);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}

