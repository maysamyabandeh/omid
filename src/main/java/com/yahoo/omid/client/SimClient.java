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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.net.InetAddress;

import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.apache.zookeeper.ZooKeeper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import com.yahoo.omid.IsolationLevel;
import com.yahoo.omid.OmidConfiguration;
import com.yahoo.omid.tso.TSOMessage;
import com.yahoo.omid.tso.RowKey;
import com.yahoo.omid.tso.messages.CommitResponse;
import com.yahoo.omid.tso.messages.PrepareResponse;
import com.yahoo.omid.tso.messages.TimestampResponse;
import com.yahoo.omid.tso.messages.CommitRequest;
import com.yahoo.omid.tso.messages.MultiCommitRequest;
import com.yahoo.omid.tso.messages.PrepareCommit;
import com.yahoo.omid.tso.messages.CommitQueryResponse;
import org.jboss.netty.channel.Channel;
import java.io.IOException;
import java.net.UnknownHostException;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.Channels;
import java.util.concurrent.TimeUnit;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelFuture;
import java.util.concurrent.CountDownLatch;


/**
 * Simple Transaction Client using Serialization
 * @author maysam
 *
 */
public class SimClient {
    /**
     * Static fields
     */
    private static final Log LOG = LogFactory.getLog(SimClient.class);

    /**
     * Maximum number of modified rows in each transaction
     */
    static int GLOBAL_MAX_ROW = 20;

    /**
     * How much wait between connect and launching the traffic
     */
    static int WAIT_AFTER_CONNECT = 1000;//15000

    /**
     * The number of rows in database
     */
    static final int DB_SIZE = 20000000;

    //private static final long PAUSE_LENGTH = 50; // in ms
    static final long PAUSE_LENGTH = 50000; // in micro sec

    /**
     * Maximum number if outstanding message
     */
    static int MAX_IN_FLIGHT = 10;

    /**
     * Number of message to do
     */
    static long nbMessage;

    static boolean pauseClient = false;

    static float percentReads = 0;

    static private int QUERY_RATE = 100;// send a query after this number of commit
    // requests

    static ScheduledExecutorService executor = null;

    /**
     * Non-static fields
     */
    /**
     * The interface to the tso
     */
    SimTSOClient[] tsoClients;
    /**
     * The interface to the sequencer
     */
    TSOClient sequencerClient;

    /**
     * Start date
     */
    private Date startDate = null;

    /**
     * Stop date
     */
    private Date stopDate = null;

    /**
     * Barrier for all clients to be finished
     */
    CountDownLatch latch;

    private java.util.Random rnd;

    /**
     * Method to wait for the final response
     * 
     * @return success or not
     */
    public boolean waitForAll() {
        while (latch.getCount() > 0) {
            try {
                latch.await();
            } catch (InterruptedException e) {
                // Ignore.
            }
        }
        stopDate = new Date();
        String MB = String.format("Memory Used: %8.3f MB", (Runtime.getRuntime().totalMemory() - Runtime.getRuntime()
                    .freeMemory()) / 1048576.0);
        //String Mbs = String.format("%9.3f TPS",
                //((nbMessage - curMessage) * 1000 / (float) (stopDate.getTime() - (startDate != null ? startDate.getTime()
                        //: 0))));
        //TODO: make curMessage global
        //System.out.println(MB + " " + Mbs);
        return true;
    }

    /**
     * Constructor
     */
    public SimClient(Properties[] soConfs, Properties sequencerConf, int index) throws IOException {
        try {
            latch = new CountDownLatch(1 + soConfs.length);
            int id = generateUniqueId(index);
            sequencerClient = new SimSequencerClient(sequencerConf, id);
            tsoClients = new SimTSOClient[soConfs.length];
            for (int i = 0; i < soConfs.length; i++)
                tsoClients[i] = new SimTSOClient(soConfs[i], id);
        } catch (IOException e) {
            latch = new CountDownLatch(0);
        }
    }

    int generateUniqueId(int i) {
        int id = 0;
        try {
            InetAddress thisIp = InetAddress.getLocalHost();
            id = thisIp.hashCode();
            id = id * (i+1);//avoid i = 0
            LOG.warn("Generate Id: " + id);
            return id;
        } catch (UnknownHostException e) {
            LOG.error("Error in generating the unique id" + e);
            e.printStackTrace();
            System.exit(1);
        }
        return id;
    }

    class SimSequencerClient extends SimTSOClient {
        static final boolean INTRODUCE = true;
        public SimSequencerClient(Properties conf, int id) throws IOException {
            super(conf, id, INTRODUCE==false);
        }

        /**
         * Starts the traffic
         */
        @Override
        public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
            super.channelConnected(ctx, e);
            startDate = new Date();
            synchronized(SimClient.this) {
                if (rnd == null) {
                    long seed = System.currentTimeMillis();
                    seed *= e.getChannel().getId();// to make it channel dependent
                    rnd = new java.util.Random(seed);
                }
            }
        }

        /**
         * Launch the benchmark
         */
        @Override
        protected void launchBenchmark() {
            launchGlobalBenchmark();
        }

        private void launchGlobalBenchmark() {
            for (int i = 0; i < MAX_IN_FLIGHT; i++) {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
while (curMessage > 0) {
    curMessage--;
    try {
        //1. get a vector timestamp
        PingPongCallback<TimestampResponse>[] tscbs;
        tscbs = new PingPongCallback[tsoClients.length];
        long sequence = sequenceGenerator.getAndIncrement();
        for (int i = 0; i < tsoClients.length; i++)
            tscbs[i] = tsoClients[i].registerTimestampCallback(sequence);
        getNewIndirectTimestamp(sequence);
        boolean failed = false;
        for (int i = 0; i < tsoClients.length; i++) {
            tscbs[i].await();
            if (tscbs[i].getException() != null)
                failed = true;
        }
        if (failed)
            throw new TransactionException("Error retrieving timestamp", null);
        //2. run the transation
        ReadWriteRows[] rw = new ReadWriteRows[tsoClients.length];
        int remainedSize = GLOBAL_MAX_ROW;
        for (int i = 0; i < tsoClients.length; i++) {
            float fshare = remainedSize / (float)(tsoClients.length - i);
            int share = (int)Math.ceil(fshare);//use ceil to avoid 0 share
            remainedSize -= share;
            rw[i] = tsoClients[i].simulateATransaction(tscbs[i].getPong().timestamp,
                    share);
        }
        //3. send prepares
        PingPongCallback<PrepareResponse>[] prcbs;
        prcbs = new PingPongCallback[tsoClients.length];
        for (int i = 0; i < tsoClients.length; i++) {
            prcbs[i] = new PingPongCallback<PrepareResponse>();
            long ts = tscbs[i].getPong().timestamp;
            PrepareCommit pcmsg = new PrepareCommit(ts, rw[i].writtenRows, rw[i].readRows);
            tsoClients[i].prepareCommit(ts, pcmsg, prcbs[i]);
        }
        boolean success = true;
        for (int i = 0; i < tsoClients.length; i++) {
            prcbs[i].await();
            success = success && prcbs[i].getPong().committed;
        }
        //4. get a vector commit timestamp
        PingPongCallback<CommitResponse>[] tccbs;
        tccbs = new PingPongCallback[tsoClients.length];
        for (int i = 0; i < tsoClients.length; i++)
            tccbs[i] = tsoClients[i].registerCommitCallback(tscbs[i].getPong().timestamp);
        long[] vts = new long[tsoClients.length];
        for (int i = 0; i < tsoClients.length; i++)
            vts[i] = tscbs[i].getPong().timestamp;
        MultiCommitRequest mcr = new MultiCommitRequest(vts);
        mcr.prepared = true;
        mcr.successfulPrepared = success;
        getNewIndirectCommitTimestamp(mcr);
        failed = false;
        for (int i = 0; i < tsoClients.length; i++) {
            tccbs[i].await();
            if (tscbs[i].getException() != null)
                failed = true;
        }
        if (failed)
            throw new TransactionException("Error committing", null);
        //System.out.print("+");
    } catch (TransactionException e) {
        System.out.print("-");
    } catch (InterruptedException e) {
        e.printStackTrace();
    } catch (IOException e) {
        LOG.error("Couldn't start transaction", e);
    } catch (Exception e) {
        e.printStackTrace();
        System.exit(1);
    }
}
                    }
                });
            }
        }

    }

    class SimTSOClient extends TSOClient {
        /**
         * Current rank (decreasing, 0 is the end of the game)
         */
        long curMessage;

        /*
         * For statistial purposes
         */
        ConcurrentHashMap<Long, Long> wallClockTime = new ConcurrentHashMap<Long, Long>();
        public long totalNanoTime = 0;
        public long totalTx = 0;
        private long totalSimulatedTxns = 0;

        static final boolean INTRODUCE = true;
        public SimTSOClient(Properties conf, int id) throws IOException {
            this(conf, id, INTRODUCE==true);
        }
        public SimTSOClient(Properties conf, int id, boolean introduceYourself) throws IOException {
            super(conf, id, introduceYourself);
            this.curMessage = nbMessage;
        }

        /**
         * Starts the traffic
         */
        @Override
        public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
            super.channelConnected(ctx, e);
            try {
                //wait for other clients to connect
                Thread.sleep(WAIT_AFTER_CONNECT);
            } catch (InterruptedException e1) {
                //ignore
            }
            synchronized(SimClient.this) {
                if (rnd == null) {
                    long seed = System.currentTimeMillis();
                    seed *= e.getChannel().getId();// to make it channel dependent
                    rnd = new java.util.Random(seed);
                }
            }
            launchBenchmark();
        }

        /**
         * When the channel is closed, print result
         */
        @Override
        public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
            super.channelClosed(ctx, e);
            latch.countDown();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
            e.getCause().printStackTrace();
            Channels.close(e.getChannel());
        }

        /**
         * Sends the CommitRequest message to the channel
         * 
         * @param timestamp
         * @param channel
         */
        public void sendCommitRequest(CommitRequest msg, final PingPongCallback<CommitResponse> tccb) {
            //if (slowchance == -1) {
            //slowchance = rnd.nextInt(10);
            //if (slowchance == 0)
            //System.out.println("I am slow");
            //}
            long randompausetime = pauseClient ? PAUSE_LENGTH : 0; //this is the average
            double uniformrandom = rnd.nextDouble(); //[0,1)
            //double geometricrandom = -1 * java.lang.Math.log(uniformrandom);
            //randompausetime = (long) (randompausetime * geometricrandom);
            randompausetime = (long) (randompausetime * 2 * uniformrandom);
            //if (slowchance == 0)
            //randompausetime = 1000 * randompausetime;
            try {
                Thread.sleep(randompausetime / 1000);//millisecond
            } catch (InterruptedException e1) {
                //ignore
            }
            // keep statistics
            wallClockTime.put(msg.getStartTimestamp(), System.nanoTime());
            try {
                commit(msg.getStartTimestamp(), msg, tccb);
            } catch (IOException e) {
                LOG.error("Couldn't send commit", e);
                e.printStackTrace();
            }
        }

        /**
         * Sends the CommitRequest message to the channel
         * 
         * @param timestamp
         * @param channel
         */
        private void sendCommitRequest(final long timestamp, final PingPongCallback<CommitResponse> tccb) {
            RowKey [] writtenRows, readRows;
            ReadWriteRows rw = simulateATransaction(timestamp, GLOBAL_MAX_ROW);
            CommitRequest crmsg = new CommitRequest(timestamp, rw.writtenRows, rw.readRows);
            sendCommitRequest(crmsg, tccb);
        }

        /**
         * Sends the CommitRequest message to the channel
         * 
         * @param timestamp
         * @param channel
         */
         ReadWriteRows simulateATransaction(long timestamp, int MAX_ROW) {
            // initialize rnd if it is not yet
            assert(rnd != null);

            boolean readOnly = (rnd.nextFloat() * 100) < percentReads;

            int writtenSize = MAX_ROW == 0 ? 0 : rnd.nextInt(MAX_ROW);
            int readSize = writtenSize == 0 ? 0 : rnd.nextInt(MAX_ROW);
            if (!IsolationLevel.checkForReadWriteConflicts)
                readSize = 0;

            final RowKey [] writtenRows = new RowKey[writtenSize];
            for (int i = 0; i < writtenRows.length; i++) {
                // long l = rnd.nextLong();
                long l = rnd.nextInt(DB_SIZE);
                byte[] b = new byte[8];
                for (int iii = 0; iii < 8; iii++) {
                    b[7 - iii] = (byte) (l >>> (iii * 8));
                }
                byte[] tableId = new byte[8];
                writtenRows[i] = new RowKey(b, tableId);
            }

            final RowKey [] readRows = new RowKey[readSize];
            for (int i = 0; i < readRows.length; i++) {
                // long l = rnd.nextLong();
                long l = rnd.nextInt(DB_SIZE);
                byte[] b = new byte[8];
                for (int iii = 0; iii < 8; iii++) {
                    b[7 - iii] = (byte) (l >>> (iii * 8));
                }
                byte[] tableId = new byte[8];
                readRows[i] = new RowKey(b, tableId);
            }

            // send a query once in a while
            totalSimulatedTxns++;
            if (totalSimulatedTxns % QUERY_RATE == 0 && writtenRows.length > 0) {
                long queryTimeStamp = rnd.nextInt(Math.abs((int) timestamp));
                try {
                    isCommitted(timestamp, queryTimeStamp, new PingPongCallback<CommitQueryResponse>());
                } catch (IOException e) {
                    LOG.error("Couldn't send commit query", e);
                }
            }
            return new ReadWriteRows(readRows, writtenRows);
        }

        class ReadWriteRows {
            RowKey[] readRows, writtenRows;
            ReadWriteRows(RowKey[] r, RowKey[] w) {
                readRows = r;
                writtenRows = w;
            }
        }

        protected void launchBenchmark() {
            launchLocalBenchmark();
        }

        /**
         * Launch the benchmark
         */
        private void launchLocalBenchmark() {
            for (int i = 0; i < MAX_IN_FLIGHT; i++) {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        while (curMessage > 0) {
                            curMessage--;
                            final PingPongCallback<TimestampResponse> tscb = new PingPongCallback<TimestampResponse>();
                            try {
                                getNewTimestamp(tscb);
                                tscb.await();
                                if (tscb.getException() != null)
                                    throw tscb.getException();
                                TimestampResponse pong = tscb.getPong();
                                PingPongCallback<CommitResponse> tccb = new PingPongCallback<CommitResponse>();
                                sendCommitRequest( pong.timestamp, tccb );
                                tccb.await();
                                handle(tccb.getPong());
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            } catch (IOException e) {
                                LOG.error("Couldn't start transaction", e);
                            } catch (Exception e) {
                                LOG.error("Unexpected Exception", e);
                                e.printStackTrace();
                            }
                        }
                    }
                });
            }
        }

        /**
         * Handle the CommitRequest message
         */
        private long lasttotalTx = 0;
        private long lasttotalNanoTime = 0;
        private long lastTimeout = System.currentTimeMillis();
        private void handle(CommitResponse msg) {
            long finishNanoTime = System.nanoTime();
            long startNanoTime = wallClockTime.remove(msg.startTimestamp);
            if (msg.committed) {
                totalNanoTime += (finishNanoTime - startNanoTime);
                totalTx++;
                long timeout = System.currentTimeMillis();
                // if (totalTx % 10000 == 0) {//print out
                if (timeout - lastTimeout > 60 * 1000) { // print out
                    long difftx = totalTx - lasttotalTx;
                    long difftime = totalNanoTime - lasttotalNanoTime;
                    System.out.format(
                            " CLIENT: totalTx: %d totalNanoTime: %d microtime/tx: %4.3f tx/s %4.3f "
                            + "Memory Used: %8.3f KB TPS:  %9.3f \n",
                            difftx,
                            difftime,
                            (difftime / (double) difftx / 1000),
                            1000 * difftx / ((double) (timeout - lastTimeout)),
                            (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024.0,
                            ((nbMessage - curMessage) * 1000 / (float) (new Date().getTime() - (startDate != null ? startDate
                                    .getTime() : 0))));
                    lasttotalTx = totalTx;
                    lasttotalNanoTime = totalNanoTime;
                    lastTimeout = timeout;
                }
                //report the reincarnation
                if (msg.rowsWithWriteWriteConflict != null) {
                    for (RowKey r: msg.rowsWithWriteWriteConflict)
                        LOG.warn("WW " + msg.startTimestamp + " " + msg.commitTimestamp + " row is: ");
                    try {
                        completeReincarnation(msg.startTimestamp, PingCallback.DUMMY);
                    } catch (IOException e) {
                        LOG.error("Couldn't send reincarnation report", e);
                    }
                }
            } else {// aborted
                try {
                    completeAbort(msg.startTimestamp, PingCallback.DUMMY);
                } catch (IOException e) {
                    LOG.error("Couldn't send abort", e);
                }
            }
        }

    }

    //static int slowchance = -1;

    private void startGlobalTransaction() {
    }

    /**
     * Main class for Client taking from two to more arguments<br>
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // Print usage if no argument is specified.
        if (args.length < 2 || args.length > 9) {
            System.err
                .println("Usage: " +
                        SimClient.class.getSimpleName() +
                        " <host> <port> [-zk zk] [<number of messages>] [<MAX_IN_FLIGHT>] [<connections>] [<pause>] [<% reads>]");
            return;
        }

        // Parse options.
        String host = args[0];
        String port = args[1];
        int nextParam = 2;

        String zkServers = null;
        if (args.length > nextParam && args[nextParam].equals("-zk")) {
            nextParam++;
            zkServers = args[nextParam];
            nextParam++;
        }

        if (args.length > nextParam) {
            nbMessage = Long.parseLong(args[nextParam]);
            nextParam++;
        } else {
            nbMessage = 256;
        }
        if (args.length > nextParam) {
            MAX_IN_FLIGHT = Integer.parseInt(args[nextParam]);
            nextParam++;
        }

        int runs = 1;
        if (args.length > nextParam) {
            runs = Integer.parseInt(args[nextParam]);
            nextParam++;
        }

        if (args.length > nextParam) {
            GLOBAL_MAX_ROW = Integer.parseInt(args[nextParam]);
            nextParam++;
        }

        if (args.length > nextParam) {
            pauseClient = Boolean.parseBoolean(args[nextParam]);
            nextParam++;
        }

        if (args.length > nextParam) {
            percentReads = Float.parseFloat(args[nextParam]);
            nextParam++;
        }

        List<SimClient> handlers = new ArrayList<SimClient>();

        System.out.println("PARAM GLOBAL_MAX_ROW: " + GLOBAL_MAX_ROW);
        System.out.println("PARAM DB_SIZE: " + DB_SIZE);
        System.out.println("PARAM MAX_IN_FLIGHT: " + MAX_IN_FLIGHT);
        System.out.println("pause " + pauseClient);
        System.out.println("readPercent " + percentReads);

        executor = Executors.newScheduledThreadPool((runs+1)*MAX_IN_FLIGHT);

        OmidConfiguration conf = OmidConfiguration.create();
        conf.loadServerConfs(zkServers);
        for(int i = 0; i < runs; ++i) {
            // Create the associated Handler
            SimClient handler = new SimClient(conf.getStatusOracleConfs(), conf.getSequencerConf(), i);
            handlers.add(handler);
            if ((i - 1) % 20 == 0) Thread.sleep(1000);
        }

        // Wait for the Traffic to finish
        for (SimClient handler : handlers) {
            boolean result = handler.waitForAll();
            System.out.println("Result: " + result);
        }
    }
}
