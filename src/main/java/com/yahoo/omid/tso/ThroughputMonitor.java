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

import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.yahoo.omid.client.TSOClient;
import com.yahoo.omid.client.TransactionalTable;

/**
 * Class for Throughput Monitoring
 * @author fbregier
 *
 */
public class ThroughputMonitor extends Thread {
   private static final Log LOG = LogFactory.getLog(ThroughputMonitor.class);
   
   TSOState state;
   
   /**
    * Constructor
    */
   public ThroughputMonitor(TSOState state) {
      this.state = state;
   }
   
   @Override
   public void run() {
      if (!LOG.isTraceEnabled()) {
         return;
      }
      try {
         long oldCounter = TSOHandler.txnCnt;
         long oldGlobalCounter = TSOHandler.globaltxnCnt;
         long oldAbortCount = TSOHandler.abortCount;
         long oldGlobalAbortCount = TSOHandler.globalabortCount;
         long oldOutOfOrderCnt = TSOHandler.outOfOrderCnt;
         long oldHitCount = TSOHandler.hitCount;
         long startTime = System.currentTimeMillis();
         //            long oldWaitTime = TSOHandler.waitTime; 
         //long oldtotalowned = CommitHashMap.gettotalowned(); 
         //long oldtotaldisowned = CommitHashMap.gettotaldisowned(); 
         long oldtotalput = CommitHashMap.gettotalput(); 
         long oldtotalget = CommitHashMap.gettotalget(); 
         long oldtotalwalkforget = CommitHashMap.gettotalwalkforget(); 
         long oldtotalwalkforput = CommitHashMap.gettotalwalkforput(); 
         long oldfull = TSOMessageBuffer.itWasFull;
         long oldflushes = TSOSharedMessageBuffer._flushes;
         long oldforcedflushes = TSOSharedMessageBuffer._forcedflushes;
         long oldflusheSize = TSOSharedMessageBuffer._flSize;
         long oldwaited = TSOMessageBuffer.waited;
         long old1B = TSOSharedMessageBuffer._1B;
         long old2B = TSOSharedMessageBuffer._2B;
         long oldAB = TSOSharedMessageBuffer._AB;
         long oldAS = TSOSharedMessageBuffer._AS;
         long oldLL = TSOSharedMessageBuffer._LL;
         long oldComs = TSOSharedMessageBuffer._Coms;
         long oldHa = TSOSharedMessageBuffer._ha;
         long oldFa = TSOSharedMessageBuffer._fa;
         long oldLi = TSOSharedMessageBuffer._li;
         long oldWrites = TSOSharedMessageBuffer._Writes;
         long oldEmptyFlushes = TSOSharedMessageBuffer._emptyFlushes;
         
         long oldAskedTSO = TSOClient.askedTSO;
         long oldQueries = TSOHandler.queries;
         long oldElementsRead = TransactionalTable.elementsRead;
         long oldExtraGetsPerformed = TransactionalTable.extraGetsPerformed;
         
         long oldOverflow = TSOSharedMessageBuffer._overflows;
         for (;;) {
            Thread.sleep(3000);
            
            long endTime = System.currentTimeMillis();
            long newCounter = TSOHandler.txnCnt;
            long newGlobalCounter = TSOHandler.globaltxnCnt;
            long newAbortCount = TSOHandler.abortCount;
            long newGlobalAbortCount = TSOHandler.globalabortCount;
            long newOutOfOrderCnt = TSOHandler.outOfOrderCnt;
            long newHitCount = TSOHandler.hitCount;
            //long newtotalowned = CommitHashMap.gettotalput(); 
            //long newtotaldisowned = CommitHashMap.gettotalget(); 
            long newtotalput = CommitHashMap.gettotalput(); 
            long newtotalget = CommitHashMap.gettotalget(); 
            long newtotalwalkforget = CommitHashMap.gettotalwalkforget(); 
            long newtotalwalkforput = CommitHashMap.gettotalwalkforput();

            long newfull = TSOMessageBuffer.itWasFull;
            long newflushes = TSOSharedMessageBuffer._flushes;
            long newforcedflushes = TSOSharedMessageBuffer._forcedflushes;
            long newflusheSize = TSOSharedMessageBuffer._flSize;
            long newwaited = TSOMessageBuffer.waited;
            
            long new1B = TSOSharedMessageBuffer._1B;
            long new2B = TSOSharedMessageBuffer._2B;
            long newAB = TSOSharedMessageBuffer._AB;
            long newAS = TSOSharedMessageBuffer._AS;
            long newLL = TSOSharedMessageBuffer._LL;
            long newComs = TSOSharedMessageBuffer._Coms;
            long newHa = TSOSharedMessageBuffer._ha;
            long newFa = TSOSharedMessageBuffer._fa;
            long newLi = TSOSharedMessageBuffer._li;
            long newWrites = TSOSharedMessageBuffer._Writes;
            double avg = TSOSharedMessageBuffer._Avg;
            double avg2 = TSOSharedMessageBuffer._Avg2;
            
            long newOverflow = TSOSharedMessageBuffer._overflows;
            long newEmptyFlushes = TSOSharedMessageBuffer._emptyFlushes;
            

            long newQueries = TSOHandler.queries;
            long newElementsRead = TransactionalTable.elementsRead;
            long newExtraGetsPerformed = TransactionalTable.extraGetsPerformed;
            long newAskedTSO = TSOClient.askedTSO;

            if (TSOPipelineFactory.bwhandler != null) {
                TSOPipelineFactory.bwhandler.measure();
            }
            LOG.trace(String.format("SERVER: %4.1f (%4.1f) TPS(G), %4.1f (%4.1f) Abort/s(G), %4.1f rjctd/s "
                    + "Tot overflows: %d Tot flushes: %d Tot empty flu: %d "
                    + "Queries: %d CurrentBuffers: %d ExtraGets: %d AskedTSO: %d Tot fflushes: %d"
                    + "Avg commit: %2.4f Avg flush: %5.2f "
                    + "Avg write: %5.2f Tot writes: %d Rec Bytes/s: %5.2fMBs Sent Bytes/s: %5.2fMBs %d ",
                    //+ "Owned %d Disowned %d ",
                    (newCounter - oldCounter) / (float)(endTime - startTime) * 1000,
                    (newGlobalCounter - oldGlobalCounter) / (float)(endTime - startTime) * 1000,
                    (newAbortCount - oldAbortCount) / (float)(endTime - startTime) * 1000,
                    (newGlobalAbortCount - oldGlobalAbortCount) / (float)(endTime - startTime) * 1000,
                    (newOutOfOrderCnt - oldOutOfOrderCnt) / (float)(endTime - startTime) * 1000,
                    newOverflow - oldOverflow,
                    (newflushes - oldflushes),
                    newEmptyFlushes - oldEmptyFlushes,
                    newQueries - oldQueries,
                    TSOBuffer.nBuffers,
                    newExtraGetsPerformed - oldExtraGetsPerformed,
                    newAskedTSO - oldAskedTSO,
                    (newforcedflushes - oldforcedflushes),
                    avg,
                    (newflusheSize - oldflusheSize) / (float)(newflushes - oldflushes),
                    avg2,
                    (newWrites - oldWrites),
                    TSOPipelineFactory.bwhandler != null ? TSOPipelineFactory.bwhandler.getBytesReceivedPerSecond() / (double) (1024 * 1024) : 0,
                    TSOPipelineFactory.bwhandler != null ? TSOPipelineFactory.bwhandler.getBytesSentPerSecond() / (double) (1024 * 1024) : 0,
                    state.largestDeletedTimestamp
                    //newtotalowned, newtotaldisowned
                    )
              );
            
            oldCounter = newCounter;
            oldGlobalCounter = newGlobalCounter;
            oldAbortCount = newAbortCount;
            oldGlobalAbortCount = newGlobalAbortCount;
            oldOutOfOrderCnt = newOutOfOrderCnt;
            oldHitCount = newHitCount;
            startTime = endTime;
            //                oldWaitTime = newWaitTime;
            oldtotalget = newtotalget;
            //oldtotalowned = newtotalowned;
            //oldtotaldisowned = newtotaldisowned;
            oldtotalput = newtotalput;
            oldtotalwalkforget = newtotalwalkforget;
            oldtotalwalkforput = newtotalwalkforput;
            oldfull = newfull;
            oldflushes = newflushes;
            oldforcedflushes = newforcedflushes;
            oldflusheSize = newflusheSize;
            oldwaited = newwaited;
            oldOverflow = newOverflow;
            

            old1B = new1B;
            old2B = new2B;
            oldAB = newAB;
            oldAS = newAS;
            oldLL = newLL;
            oldComs = newComs;
            oldHa = newHa;
            oldFa = newFa;
            oldLi = newLi;
            oldWrites = newWrites;
            oldEmptyFlushes = newEmptyFlushes;
            

            oldAskedTSO = newAskedTSO;
            oldQueries = newQueries;
            oldElementsRead = newElementsRead;
            oldExtraGetsPerformed = newExtraGetsPerformed;
         }
      } catch (InterruptedException e) {
         // Stop monitoring asked
         return;
      }
   }
}
