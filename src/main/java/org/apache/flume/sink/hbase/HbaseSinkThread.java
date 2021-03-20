package org.apache.flume.sink.hbase;

import org.apache.flume.Event;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.security.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class HbaseSinkThread implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(HbaseSinkThread.class);
    private LinkedBlockingQueue<Event> bufferQueue;
    private CyclicBarrier barrier;
    private HbaseEventSerializer serializer;
    private byte[] columnFamily;
    private User hbaseUser;
    private volatile String tableName;
    private long batchSize;
    private Configuration config;

    public HbaseSinkThread(LinkedBlockingQueue bufferQueue, CyclicBarrier barrier, HbaseEventSerializer serializer, byte[] columnFamily, User hbaseUser, String tableName, long batchSize, Configuration config) {
        this.bufferQueue = bufferQueue;
        this.barrier = barrier;
        this.serializer = serializer;
        this.columnFamily = columnFamily;
        this.hbaseUser = hbaseUser;
        this.tableName = tableName;
        this.batchSize = batchSize;
        this.config = config;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }


    @Override
    public void run() {
        HTable hTable = null;
        try {
            hTable = new HTable(config, tableName);
            hTable.setWriteBufferSize(5242880);
            hTable.setAutoFlush(false);
            logger.info(Thread.currentThread().getName() + "===============HbaseSinkThread==========================" + hTable);

        } catch (IOException e) {
            e.printStackTrace();
        }


        List<Row> actions = new LinkedList<Row>();
        List<Increment> incs = new LinkedList<Increment>();

        Event event = null;
        int batchSize2 = 0;
        while (true) {
            try {
                event = bufferQueue.poll(1000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (event != null) {

                serializer.initialize(event, columnFamily);
                actions.addAll(serializer.getActions());
                incs.addAll(serializer.getIncrements());
                batchSize2++;

                if (batchSize2 > batchSize) {
                    try {
                        putEventsAndCommit(actions, hTable);
                        actions.clear();
                        incs.clear();

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    batchSize2 = 0;
                }
            } else {
                // event == null 表示 bufferQueue 中已经没有数据了，
                // 且 barrier wait 大于 0 表示当前正在执行 Checkpoint，
                // client 需要执行 flush，保证 Checkpoint 之前的数据都消费完成
                //System.out.println(barrier.getNumberWaiting());
                if (barrier.getNumberWaiting() > 0) {
                    logger.info("MultiThreadConsumerClient 执行 flush, " + "当前 wait 的线程数：" + barrier.getNumberWaiting());

                    try {
                        try {
                            putEventsAndCommit(actions, hTable);
                            actions.clear();
                            incs.clear();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        batchSize2 = 0;

                        barrier.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (BrokenBarrierException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

    }


    private void putEventsAndCommit(final List<Row> actions,
                                    final HTable hTable) throws Exception {

        runPrivileged(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {

                try {
                    Long startTime = System.currentTimeMillis();
                    hTable.batch(actions);
                    Long endTime = System.currentTimeMillis();
                    logger.info(Thread.currentThread().getName() + "===tableName:" + hTable + ",count:{}, 耗时: {} ms", actions.size(), (endTime - startTime));
                } catch (Exception e) {
                    try {
                        hTable.batch(actions);
                    } catch (Exception e1) {
                        try {
//                            MonitorInfluxdb.SendMonitorClient(MonitorMsg.buildMsgInfo("flume", System.currentTimeMillis(), 2, tableName, "FLUME"), monitorUrl);
                        } catch (Exception e2) {
                            logger.error(" error : MonitorInfluxdb:" + e2);
                        }
                        logger.error("hbase tableName:" + hTable + " error msg:" + e);
                        logger.error(actions + "hbase put  failed!" + hTable + "error msg:" + e1);
                    }
                }

                return null;

            }
        });

        /*runPrivileged(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {

                List<Increment> processedIncrements;
                if (batchIncrements) {
                    processedIncrements = coalesceIncrements(incs);
                } else {
                    processedIncrements = incs;
                }

                // Only used for unit testing.
                if (debugIncrCallback != null) {
                    debugIncrCallback.onAfterCoalesce(processedIncrements);
                }

                for (final Increment i : processedIncrements) {
                    i.setWriteToWAL(enableWal);
                    table.increment(i);
                }
                return null;
            }
        });*/

    }


    private <T> T runPrivileged(final PrivilegedExceptionAction<T> action)
            throws Exception {
        if (hbaseUser != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Calling runAs as hbase user: " + hbaseUser.getName());
            }
            return hbaseUser.runAs(action);
        } else {
            return action.run();
        }
    }


}
