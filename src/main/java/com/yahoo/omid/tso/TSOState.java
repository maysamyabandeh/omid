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
import com.yahoo.omid.tso.messages.PrepareCommit;
import com.yahoo.omid.client.TSOClient;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.Iterator;
import java.util.Properties;
import java.io.IOException;
import com.yahoo.omid.tso.messages.MultiCommitRequest;
import com.yahoo.omid.sharedlog.*;
import com.yahoo.omid.tso.persistence.StateLogger;
import com.yahoo.omid.tso.persistence.SyncFileStateLogger;
import org.apache.zookeeper.ZooKeeper;
import java.util.concurrent.ScheduledExecutorService;
import com.yahoo.omid.tso.persistence.LoggerAsyncCallback.LoggerInitCallback;
import com.yahoo.omid.tso.persistence.LoggerException;
import com.yahoo.omid.tso.persistence.LoggerException.Code;
import org.jboss.netty.buffer.ChannelBuffer;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.PriorityBlockingQueue;
import com.yahoo.omid.tso.persistence.LoggerConstants;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The wrapper for different states of TSO
 * This state is shared between handlers
 * @author maysam
 */
public class TSOState {
    private static final Log LOG = LogFactory.getLog(TSOState.class);
    
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
  //depricated 
   StateLogger logger;

    /**
     * The sharedLog is a lock-free log that keeps the recent sequenced messages
     * The readers read messages from this log and send it to the registered SOs
     */
    SharedLog sharedLog;

   /**
    * There is a single writer for the log. The append method is synchronized to give
    * a serial order to the messages.
    * @author maysam
    */
   LogWriter logWriter;

   /**
    * logPersister persists the content of the sharedLog into a logBackend
    * @author maysam
    */
   LogPersister logPersister;

   /**
    * the logBackend provides persistent storage for the log content. The logBackend
    * must be accessible by braodcast registeres, i.e., status oracles.
    * @author maysam
    */
   StateLogger logBackend;

    /**
     * Init the logBackend medium
     * The users of logBackend are notified of the init completion by checking
     * that logBackend is not null
     * @author maysam
     */
    void initLogBackend(ZooKeeper zk) {
        try {
            new SyncFileStateLogger(zk).initialize(new LoggerInitCallback() {
                public void loggerInitComplete(int rc, StateLogger sl, Object ctx){
                    if(rc == Code.OK){
                        if(LOG.isDebugEnabled()){
                            LOG.debug("Logger is ok.");
                        }
                        LOG.warn("backend loggerInitComplete: OK");
                        logBackend = sl;
                    } else {
                        LOG.error("Error when initializing logger: " + LoggerException.getMessage(rc));
                    }
                }

            }, null, LoggerConstants.OMID_TSO_LOG + getId());
        } catch (Exception e) {
            e.printStackTrace();
            //TODO: react properly
            System.exit(1);
        }
    }

    /**
     * We keep a mapping between channels and logreaders.
     */
    Map<Channel, LogReader> channelToReaderMap = new ConcurrentHashMap<Channel, LogReader>();

    /**
     * This class associates a message to an index in the log
     * Is used to linger transmission of the message before the corresponding
     * index is persisted.
     */
    static class ToBeLoggedMessage implements Comparable<ToBeLoggedMessage> {
        TSOMessage msg;
        Channel channel;
        /**
         * The log index asscociated with the message.
         * @assume: globalLogPointer >= actual index corresponding to the message
         */
        long globalLogPointer;

        ToBeLoggedMessage(TSOMessage msg, Channel channel, long index) {
            this.msg = msg;
            this.channel = channel;
            this.globalLogPointer = index;
        }

        void send() {
            channel.write(msg);
            channel = null;//raise an exception if to be sent again
        }

        @Override
        public int compareTo(ToBeLoggedMessage otherMsg) {
            return (int) (globalLogPointer - otherMsg.globalLogPointer);
        }
    }

    /**
     * A priority queue that buffers the outgoing messages
     * It is thread-safe
     */
    PriorityBlockingQueue<ToBeLoggedMessage> outMsgs = new PriorityBlockingQueue();

    /**
     * after persisting some part of the log, check to see if we can let some
     * queued messages be sent out
     */
    void letPersistedMessagesGo() {
        long persistedIndex = logPersister.getGlobalPointer();
        while (true) {
            ToBeLoggedMessage firstMsg = outMsgs.poll();
            if (firstMsg != null && firstMsg.globalLogPointer >= persistedIndex)
                firstMsg.send();
            else
                break;
        }
    }

    private ScheduledExecutorService executor;
    /**
     * A thread that periodically reads from the sharedLog that persists the recent
     * writes of the sharedLog
     */
    private class PersistenceThread implements Runnable {
        LogPersister logPersister;
        public PersistenceThread(LogPersister logPersister) {
            this.logPersister = logPersister;
        }
        @Override
        public void run() {
            for (;;) {
                try {
                    if (logBackend == null) {
                        System.out.println("Wait more for the log backend ...");
                        Thread.sleep(100);
                        continue;
                    }
                    LogPersister.ToBePersistedData toBePersistedData = logPersister.toBePersisted();
                    if (toBePersistedData == null) {
                        Thread.yield();
                        continue;
                    }
                    //keep count for statistical purposes
                    TSOHandler.globaltxnCnt++;
                    ChannelBuffer tail = toBePersistedData.getData();
                    //TODO: update the logBackend to operate on ChannelBuffer
                    //this would eliminate the extra copy
                    byte[] record;
                    record = new byte[tail.readableBytes()];
                    tail.readBytes(record);
                    logBackend.addRecord(record, 
                            new AddRecordCallback() {
                                @Override
                                public void addRecordComplete(int rc, Object ctx) {
                                    if (rc != Code.OK) {
                                        LOG.error("Writing to log backend failed: " + LoggerException.getMessage(rc));
                                        System.exit(1);
                                        //TODO: handle it properly
                                    } else {
                                        //update the pointer of the last persisted data
                                        LogPersister.ToBePersistedData toBePersistedData = (LogPersister.ToBePersistedData) ctx;
                                        toBePersistedData.persisted();
                                        letPersistedMessagesGo();
                                    }
                                }
                            }, toBePersistedData);
                    Thread.sleep(TSOState.FLUSH_TIMEOUT);
                } catch (SharedLogLateFollowerException lateE) {
                    //TODO do something
                    lateE.printStackTrace();
                } catch (SharedLogException sharedE) {
                    //TODO do something
                    sharedE.printStackTrace();
                //} catch (IOException ioE) {
                    ////TODO do something
                    //ioE.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }



    //@depricated
   public StateLogger getLogger(){
       return logger;
   }
   
    //@depricated
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

   //The list of the elders: the committed transactions with write write conflicts
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
    //@depricated
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
    //@depricated
   void stop(){
       if(logger != null){
           logger.shutdown();
       }
   }
   
   public TSOState(StateLogger logger, TimestampOracle timestampOracle) {
       this(timestampOracle);
       this.logger = logger;
   }
   
   public TSOState(TimestampOracle timestampOracle) {
       this.timestampOracle = timestampOracle;
       this.largestDeletedTimestamp = timestampOracle.get();
       this.uncommited = new Uncommited(largestDeletedTimestamp);
       this.elders = new Elders();
       this.logger = null;
       startsLockMonitor();

       //init the new implementation of the shared message buffer
       this.sharedLog = new SharedLog();
       this.logWriter = new LogWriter(sharedLog);
       this.logPersister = new LogPersister(sharedLog, logWriter);
       //logWriter should be careful not to rewrite the data that is not persisted yet.
       this.logWriter.setPersister(this.logPersister);
       this.executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
           @Override
           public Thread newThread(Runnable r) {
               Thread t = new Thread(Thread.currentThread().getThreadGroup(), r);
               t.setDaemon(true);
               t.setName("Flush Thread");
               return t;
           }
       });
       executor.schedule(new PersistenceThread(logPersister), 0, TimeUnit.MILLISECONDS);
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

