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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import com.yahoo.omid.tso.messages.TimestampSnapshot;
import com.yahoo.omid.tso.messages.AbortedTransactionReport;
import com.yahoo.omid.sharedlog.LogWriter;
import com.yahoo.omid.sharedlog.SharedLogException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

/**
 * The Timestamp Oracle that gives monotonically increasing timestamps
 * 
 * @author maysam
 * 
 */

public class TimestampOracle {

    private static final Log LOG = LogFactory.getLog(TimestampOracle.class);

    private static final long TIMESTAMP_BATCH = 100000;

    private long maxTimestamp;

    /**
     * the last returned timestamp
     */
    private long last;
    private long first;

    private boolean enabled;

    ChannelBuffer tmpTsSnapshotBuffer = ChannelBuffers.buffer(50);

    /**
     * Must be called holding an exclusive lock
     * @return the next timestamp
     */
    public long next(LogWriter logWriter) throws IOException {
        last++;
        if (last >= maxTimestamp) {
            maxTimestamp += TIMESTAMP_BATCH;
            TimestampSnapshot tsnap = new TimestampSnapshot(maxTimestamp);
            //AbortedTransactionReport tsnap = new AbortedTransactionReport(maxTimestamp);
            tmpTsSnapshotBuffer.clear();
            tmpTsSnapshotBuffer.writeByte(TSOMessage.TimestampSnapshot);
            tsnap.writeObject(tmpTsSnapshotBuffer);
            try {
                logWriter.append(tmpTsSnapshotBuffer);
            } catch (SharedLogException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
        if(LOG.isTraceEnabled()){
            LOG.trace("Next timestamp: " + last);
        }
        return last;
    }

    public long get() {
        return last;
    }

    public long first() {
        return first;
    }

    private static final String BACKUP = "/tmp/tso-persist.backup";
    private static final String PERSIST = "/tmp/tso-persist.txt";

    /**
     * Constructor
     */
    public TimestampOracle(){
        this.enabled = false;
        //make sure you do not use timestamp 0. It triggers a bug in HBase. Since the first time we use last++, initializing it to 0 is fine.
        this.last = 0;
        initialize();
    }

    /**
     * Starts from scratch.
     */
    private void initialize(){
       this.enabled = true;
       //enable the old scheme of reading the last timestmap from a file
       //initFromFile();
    }

    /**
     * Starts with a given timestamp.
     * @param timestamp upperbound of previously used timestamps
     */
    public void initialize(long timestamp){
        LOG.info("Initializing timestamp oracle");
        this.last = this.first = timestamp;
        this.maxTimestamp = this.last + TIMESTAMP_BATCH;
        LOG.info("First: " + this.first + ", Last: " + this.last);
        this.enabled = true;
    }
    
    /**
     * initialize the last timestamp 
     */
    public void initFromFile() {
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(PERSIST));
            first = last = Long.parseLong(reader.readLine()) + TIMESTAMP_BATCH;
        } catch (FileNotFoundException e) {
            LOG.error("Couldn't read persisted timestamp from /tmp/tso-persist.txt");
        } catch (NumberFormatException e) {
            LOG.error("File /tmp/tso-persist.txt doesn't contain a number");
        } catch (IOException e) {
            LOG.error("Couldn't read persisted timestamp from /tmp/tso-persist.txt");
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    LOG.error("Error closing file: " + e);
                }
            }
            if (last == 0)
               last = 1;//to bypass the bug in HBase with version 0, I load the db with version 1
            first = last;
            maxTimestamp = first + TIMESTAMP_BATCH;
        }

        Thread flusher = new Thread(new Runnable() {
            @Override
            public void run() {
                while (enabled) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        return;
                    }

                    BufferedWriter writer = null;
                    try {
                        writer = new BufferedWriter(new FileWriter(BACKUP));
                        writer.write(Long.toString(last));
                    } catch (IOException e) {
                        LOG.error("Couldn't overwrite persisted timestamp");
                        return;
                    } finally {
                        if (writer != null)
                            try {
                                writer.close();
                            } catch (IOException e) {
                                LOG.error("Couldn't overwrite persisted timestamp");
                            }
                    }

                    File backup = new File(BACKUP);
                    File persist = new File(PERSIST);
                    if (!backup.renameTo(persist))
                        LOG.error("Couldn't overwrite persisted timestamp");

                    if (Thread.interrupted()) {
                        return;
                    }
                }

            }
        });
        flusher.start();
    }

    public void stop() {
        this.enabled = false;
    }

    @Override
    public String toString() {
        return "TimestampOracle: " + last;
    }
}
