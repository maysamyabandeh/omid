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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.apache.zookeeper.ZooKeeper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import com.yahoo.omid.IsolationLevel;
import com.yahoo.omid.tso.TSOMessage;
import com.yahoo.omid.tso.RowKey;
import com.yahoo.omid.tso.messages.CommitResponse;
import com.yahoo.omid.tso.messages.TimestampResponse;
import com.yahoo.omid.tso.messages.CommitRequest;
import com.yahoo.omid.tso.messages.CommitQueryResponse;
import org.jboss.netty.channel.Channel;
import java.io.IOException;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.Channels;
import java.util.concurrent.TimeUnit;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelFuture;


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
    static int MAX_ROW = 20;

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

    /**
     * Non-static fields
     */
    TSOClient tsoclient;

    /**
     * Current rank (decreasing, 0 is the end of the game)
     */
    private long curMessage;

    /**
     * number of outstanding commit requests
     */
    private int outstandingTransactions = 0;

    /**
     * Start date
     */
    private Date startDate = null;

    /**
     * Stop date
     */
    private Date stopDate = null;

    /**
     * Return value for the caller
     */
    final BlockingQueue<Boolean> answer = new LinkedBlockingQueue<Boolean>();

    private Channel channel;

    /*
     * For statistial purposes
     */
    ConcurrentHashMap<Long, Long> wallClockTime = new ConcurrentHashMap<Long, Long>(); 
    public long totalNanoTime = 0;
    public long totalTx = 0;

    /**
     * Method to wait for the final response
     * 
     * @return success or not
     */
    public boolean waitForAll() {
        for (;;) {
            try {
                return answer.take();
            } catch (InterruptedException e) {
                // Ignore.
            }
        }
    }

    /**
     * Constructor
     */
    public SimClient(Configuration conf) throws IOException {
        this.curMessage = nbMessage;
        tsoclient = new TSOClient(conf) {
            /**
             * Starts the traffic
             */
            @Override
            public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
                super.channelConnected(ctx, e);
                try {
                    Thread.sleep(15000);
                } catch (InterruptedException e1) {
                    //ignore
                }
                startDate = new Date();
                channel = e.getChannel();
                //Starts the traffic
                startTransaction();
            }

            /**
             * If write of Commit Request was not possible before, just do it now
             */
            @Override
            public void channelInterestChanged(ChannelHandlerContext ctx, ChannelStateEvent e) {
                startTransaction();
            }

            /**
             * When the channel is closed, print result
             */
            @Override
            public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
                super.channelClosed(ctx, e);
                terminate();
            }

            /**
             * Furthur processing on messages
             * @throws IOException 
             */
            @Override
            protected void processMessage(TSOMessage msg) {
                if (msg instanceof CommitResponse) {
                    handle((CommitResponse) msg);
                } else if (msg instanceof TimestampResponse) {
                    sendCommitRequest( ((TimestampResponse)msg).timestamp );
                }
            }

            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
                e.getCause().printStackTrace();
                answer.offer(false);
                Channels.close(e.getChannel());
            }
        };
    }

    private java.util.Random rnd;

    /**
     * Sends the CommitRequest message to the channel
     * 
     * @param timestamp
     * @param channel
     */
    private void sendCommitRequest(final long timestamp) {
        // initialize rnd if it is not yet
        if (rnd == null) {
            long seed = System.currentTimeMillis();
            seed *= channel.getId();// to make it channel dependent
            rnd = new java.util.Random(seed);
        }

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
        totalCommitRequestSent++;
        if (totalCommitRequestSent % QUERY_RATE == 0 && writtenRows.length > 0) {
            long queryTimeStamp = rnd.nextInt(Math.abs((int) timestamp));
            try {
                tsoclient.isCommitted(timestamp, queryTimeStamp, new PingPongCallback<CommitQueryResponse>());
            } catch (IOException e) {
                LOG.error("Couldn't send commit query", e);
            }
        }

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
        executor.schedule(new Runnable() {
            @Override
            public void run() {
                // keep statistics
                wallClockTime.put(timestamp, System.nanoTime());

                try {
                    CommitRequest msg = new CommitRequest(timestamp, writtenRows, readRows);
                    tsoclient.commit(timestamp, msg, new PingPongCallback<CommitResponse>());
                } catch (IOException e) {
                    LOG.error("Couldn't send commit", e);
                    e.printStackTrace();
                }
            }
        }, randompausetime, TimeUnit.MICROSECONDS);

    }

    //static int slowchance = -1;

    private static ScheduledExecutorService executor = Executors.newScheduledThreadPool(20);

    private long totalCommitRequestSent;// just to keep the total number of
    // commitreqeusts sent
    private int QUERY_RATE = 100;// send a query after this number of commit
    // requests

    /**
     * Start a new transaction
     * 
     * @param channel
     * @throws IOException 
     */
    private void startTransaction() {
        while (true) {// fill the pipe with as much as request you can
            if (outstandingTransactions >= MAX_IN_FLIGHT)
                return;

            if (curMessage == 0) {
                LOG.warn("No more message");
                // wait for all outstanding msgs and then close the channel
                if (outstandingTransactions == 0) {
                    LOG.warn("Close channel");
                    channel.close().addListener(new ChannelFutureListener() {
                        public void operationComplete(ChannelFuture future) {
                            answer.offer(true);
                        }
                    });
                }
                return;
            }
            curMessage--;
            outstandingTransactions++;
            try {
                tsoclient.getNewTimestamp(new PingPongCallback<TimestampResponse>());
            } catch (IOException e) {
                LOG.error("Couldn't start transaction", e);
            }

            Thread.yield();
        }
    }
    /**
     * Handle the CommitRequest message
     */
    private long lasttotalTx = 0;
    private long lasttotalNanoTime = 0;
    private long lastTimeout = System.currentTimeMillis();
    public void handle(CommitResponse msg) {
        // outstandingTransactions.decrementAndGet();
        outstandingTransactions--;
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
                    tsoclient.completeReincarnation(msg.startTimestamp, PingCallback.DUMMY);
                } catch (IOException e) {
                    LOG.error("Couldn't send reincarnation report", e);
                }
            }
        } else {// aborted
            try {
                tsoclient.completeAbort(msg.startTimestamp, PingCallback.DUMMY);
            } catch (IOException e) {
                LOG.error("Couldn't send abort", e);
            }
        }
        startTransaction();
    }

    void terminate() {
        stopDate = new Date();
        String MB = String.format("Memory Used: %8.3f MB", (Runtime.getRuntime().totalMemory() - Runtime.getRuntime()
                    .freeMemory()) / 1048576.0);
        String Mbs = String.format("%9.3f TPS",
                ((nbMessage - curMessage) * 1000 / (float) (stopDate.getTime() - (startDate != null ? startDate.getTime()
                        : 0))));
        System.out.println(MB + " " + Mbs);
        answer.offer(false);
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
        int port = Integer.parseInt(args[1]);
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
           MAX_ROW = Integer.parseInt(args[nextParam]);
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

        //zookeeper
        String sequencerIP = null;
        int sequencerPort;
        byte[] tmp;
        if (zkServers != null) {
            try{
                ZooKeeper zk = new ZooKeeper(zkServers, 
                        Integer.parseInt(System.getProperty("SESSIONTIMEOUT", Integer.toString(10000))), 
                        null);
                tmp = zk.getData("/sequencer/ip", false, null);
                sequencerIP = new String(tmp);
                tmp = zk.getData("/sequencer/port", false, null);
                sequencerPort = Integer.parseInt(new String(tmp));
                System.out.println(sequencerIP + " " + sequencerPort);
                List<String> sos = zk.getChildren("/sos", false);
                System.out.println(sos);
                assert(sos.size() > 0);
                String soId = sos.get(0);
                tmp = zk.getData("/sos/" + soId + "/ip", false, null);
                host = new String(tmp);
                tmp = zk.getData("/sos/" + soId + "/port", false, null);
                port = Integer.parseInt(new String(tmp));
                System.out.println(host + " " + port);
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
        else
            System.out.println("no zookeeper!");

        List<SimClient> handlers = new ArrayList<SimClient>();

        Configuration conf = HBaseConfiguration.create();
        conf.set("tso.host", host);
        conf.setInt("tso.port", port);
        conf.setInt("tso.executor.threads", 10);

        System.out.println("PARAM MAX_ROW: " + MAX_ROW);
        System.out.println("PARAM DB_SIZE: " + DB_SIZE);
        System.out.println("PARAM MAX_IN_FLIGHT: " + MAX_IN_FLIGHT);
        System.out.println("pause " + pauseClient);
        System.out.println("readPercent " + percentReads);

        for(int i = 0; i < runs; ++i) {
            // Create the associated Handler
            SimClient handler = new SimClient(conf);
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
