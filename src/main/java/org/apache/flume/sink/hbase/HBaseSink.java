/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.sink.hbase;

import cn.teld.bdp.flume.FileFormat;
import cn.teld.bdp.flume.configcenter.ConfigCenterDW;
import cn.teld.bdp.flume.configcenter.ConfigCenterUrl;
import cn.teld.bdp.flume.db.util.GetTableName;
import cn.teld.bdp.flume.monitor.MonitorInfluxdb;
import cn.teld.bdp.flume.monitor.MonitorMsg;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.*;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.flume.sink.hbase.HBaseSinkConfigurationConstants.*;


/**
 * A simple sink which reads events from a channel and writes them to HBase.
 * The Hbase configuration is picked up from the first <tt>hbase-site.xml</tt>
 * encountered in the classpath. This sink supports batch reading of
 * events from the channel, and writing them to Hbase, to minimize the number
 * of flushes on the hbase tables. To use this sink, it has to be configured
 * with certain mandatory parameters:<p>
 * <tt>table: </tt> The name of the table in Hbase to write to. <p>
 * <tt>columnFamily: </tt> The column family in Hbase to write to.<p>
 * This sink will commit each transaction if the table's write buffer size is
 * reached or if the number of events in the current transaction reaches the
 * batch size, whichever comes first.<p>
 * Other optional parameters are:<p>
 * <tt>serializer:</tt> A class implementing {@link HbaseEventSerializer}.
 * An instance of
 * this class will be used to write out events to hbase.<p>
 * <tt>serializer.*:</tt> Passed in the configure() method to serializer
 * as an object of {@link Context}.<p>
 * <tt>batchSize: </tt>This is the batch size used by the client. This is the
 * maximum number of events the sink will commit per transaction. The default
 * batch size is 100 events.
 * <p>
 * <p>
 * <strong>Note: </strong> While this sink flushes all events in a transaction
 * to HBase in one shot, Hbase does not guarantee atomic commits on multiple
 * rows. So if a subset of events in a batch are written to disk by Hbase and
 * Hbase fails, the flume transaction is rolled back, causing flume to write
 * all the events in the transaction all over again, which will cause
 * duplicates. The serializer is expected to take care of the handling of
 * duplicates etc. HBase also does not support batch increments, so if
 * multiple increments are returned by the serializer, then HBase failure
 * will cause them to be re-written, when HBase comes back up.
 */
public class HBaseSink extends AbstractSink implements Configurable {
    private static final Logger logger = LoggerFactory.getLogger(HBaseSink.class);
    private String tableName;
    private byte[] columnFamily;
    private HTable table;
    private long batchSize;
    private Configuration config;
    private HbaseEventSerializer serializer;
    private String eventSerializerType;
    private Context serializerContext;
    private String kerberosPrincipal;
    private String kerberosKeytab;
    private User hbaseUser;
    private boolean enableWal = true;
    private boolean batchIncrements = false;
    private Method refGetFamilyMap = null;
    private SinkCounter sinkCounter;
    private String dbUrl;
    private String userName;
    private String password;
    private String tableType;
    private String className;
    private String sql;
    private String monitorUrl;
    Class cls;
    FileFormat fileFormat;
    byte[] msg;
    // Internal hooks used for unit testing.
    private DebugIncrementsCallback debugIncrCallback = null;

    private boolean isOpenThread = false;
    private CyclicBarrier clientBarrier;
    ThreadPoolExecutor threadPoolExecutor;
    private int DEFAULT_CLIENT_THREAD_NUM = 3;
    //数据缓冲队列的默认容量
    private final int DEFAULT_QUEUE_CAPACITY = 100000;
    private LinkedBlockingQueue<Event> bufferQueue;
    private HbaseSinkThread consumerClient;


    public HBaseSink() {
        this(HBaseConfiguration.create());
    }

    public HBaseSink(Configuration conf) {
        this.config = conf;
    }

    @VisibleForTesting
    @InterfaceAudience.Private
    HBaseSink(Configuration conf, DebugIncrementsCallback cb) {
        this(conf);
        this.debugIncrCallback = cb;
    }

    @Override
    public void start() {
        Preconditions.checkArgument(table == null, "Please call stop " +
                "before calling start on an old instance.");
        try {

            if (HBaseSinkSecurityManager.isSecurityEnabled(config)) {
                hbaseUser = HBaseSinkSecurityManager.login(config, null,
                        kerberosPrincipal, kerberosKeytab);
            }
        } catch (Exception ex) {
            sinkCounter.incrementConnectionFailedCount();
            throw new FlumeException("Failed to login to HBase using "
                    + "provided credentials.", ex);
        }

        try {
            if (tableType.equals("tableType")) {
                final long PERIOD_DAY = 24 * 60 * 60 * 1000;
                TimeZone.setDefault(TimeZone.getTimeZone("GMT+8"));
                Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT+8"));
                /*** 定制每日00:00执行方法 ***/
                calendar.set(Calendar.HOUR_OF_DAY, 0);
                calendar.set(Calendar.MINUTE, 0);
                calendar.set(Calendar.SECOND, 0);

                Date date = calendar.getTime(); //第一次执行定时任务的时间
                //如果第一次执行定时任务的时间 小于 当前的时间
                //此时要在 第一次执行定时任务的时间 加一天，以便此任务在下个时间点执行。如果不加一天，任务会立即执行。循环执行的周期则以当前时间为准
                if (date.before(new Date())) {
                    date = addDay(date, 1);
                }
                Timer timer = new Timer();
                //安排指定的任务在指定的时间开始进行重复的固定延迟执行。
                timer.schedule(new TimerTask() {
                    @Override
                    public void run() {
                        try {
                            table = new HTable(config, GetTableName.getTableName(dbUrl, userName, password, tableName, sql, monitorUrl));
                            table.setWriteBufferSize(5242880);
                            table.setAutoFlush(false);
                        } catch (IOException e1) {
                            e1.printStackTrace();
                        }
                        logger.info("----------------schedule----------------------tableName:" + table);
                    }
                }, date, PERIOD_DAY);

                //设置索引名称
                if (isOpenThread) {
                    consumerClient.setTableName(GetTableName.getTableName(dbUrl, userName, password, tableName, sql, monitorUrl));
                    logger.info(date + "定时获取ES索引并设置多线程的索引名:" + table);
                }


            }

            table = runPrivileged(new PrivilegedExceptionAction<HTable>() {
                @Override
                public HTable run() throws Exception {
                    HTable table;
                    if (tableType.equals("tableType")) {
                        table = new HTable(config, GetTableName.getTableName(dbUrl, userName, password, tableName, sql, monitorUrl));
                    } else {
                        table = new HTable(config, tableName);
                    }
                    table.setWriteBufferSize(5242880);
                    table.setAutoFlush(false);
                    return table;
                }
            });

            logger.info("------------------tableName--------------------:" + table);


/*        //原来的hbase链接
        table = runPrivileged(new PrivilegedExceptionAction<HTable>() {
          @Override
          public HTable run() throws Exception {
            HTable table = new HTable(config, tableName);
            table.setAutoFlush(false);
            // Flush is controlled by us. This ensures that HBase changing
            // their criteria for flushing does not change how we flush.
            return table;
          }
        });*/

        } catch (Exception e) {
            sinkCounter.incrementConnectionFailedCount();
            logger.error("Could not load table, " + tableName +
                    " from HBase", e);
            throw new FlumeException("Could not load table, " + tableName +
                    " from HBase", e);
        }
        try {
            if (!runPrivileged(new PrivilegedExceptionAction<Boolean>() {
                @Override
                public Boolean run() throws IOException {
                    return table.getTableDescriptor().hasFamily(columnFamily);
                }
            })) {
                throw new IOException("Table " + tableName
                        + " has no such column family " + Bytes.toString(columnFamily));
            }
        } catch (Exception e) {
            //Get getTableDescriptor also throws IOException, so catch the IOException
            //thrown above or by the getTableDescriptor() call.
            sinkCounter.incrementConnectionFailedCount();
            throw new FlumeException("Error getting column family from HBase."
                    + "Please verify that the table " + tableName + " and Column Family, "
                    + Bytes.toString(columnFamily) + " exists in HBase, and the"
                    + " current user has permissions to access that table.", e);
        }


        //是否开启多线程
        if (isOpenThread) {
            threadPoolExecutor = new ThreadPoolExecutor(DEFAULT_CLIENT_THREAD_NUM, DEFAULT_CLIENT_THREAD_NUM,
                    0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue());
            logger.info("=======start==========threadPoolExecutor========================" + threadPoolExecutor);

            // barrier 需要拦截 (DEFAULT_CLIENT_THREAD_NUM + 1) 个线程
            clientBarrier = new CyclicBarrier(DEFAULT_CLIENT_THREAD_NUM + 1);

            this.bufferQueue = Queues.newLinkedBlockingQueue(DEFAULT_QUEUE_CAPACITY);

            consumerClient = new HbaseSinkThread(bufferQueue, clientBarrier, serializer, columnFamily, hbaseUser, tableName, batchSize, config);
            for (int i = 0; i < DEFAULT_CLIENT_THREAD_NUM; i++) {
                threadPoolExecutor.execute(consumerClient);
            }
        }


        super.start();
        sinkCounter.incrementConnectionCreatedCount();
        sinkCounter.start();
    }

    private Date addDay(Date date, int num) {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT+8"));
        Calendar startDT = Calendar.getInstance(TimeZone.getTimeZone("GMT+8"));
        startDT.setTime(date);
        startDT.add(Calendar.DAY_OF_MONTH, num);
        return startDT.getTime();
    }

    @Override
    public void stop() {
        try {
            if (table != null) {
                table.close();
            }
            table = null;
        } catch (IOException e) {
            throw new FlumeException("Error closing table.", e);
        }
        sinkCounter.incrementConnectionClosedCount();
        sinkCounter.stop();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Context context) {
        //获取hbasezk的url连接ip:port,ip:port
        Map hbasezk = ConfigCenterUrl.getConfig(context.getString(HBaseSinkConfigurationConstants
                .ZK_QUORUM));
        logger.info("===========hbasezk============" + hbasezk);
        //获取数据库的信息
        Map dwMap = ConfigCenterDW.getConfig(context.getString(dwConnection));
        logger.info("===========dwMap============" + dwMap);

/*    if (StringUtils.isNotBlank(context.getString(DBURL))) {
      dbUrl = context.getString(DBURL);
    }*/

        if (dwMap != null) {
            dbUrl = "jdbc:sqlserver://" + dwMap.get(context.getString(DBURL)) + ";databaseName=" + dwMap.get(context.getString(DATABASE));
            if (StringUtils.isNotBlank(context.getString(USERNAME))) {
                userName = (String) dwMap.get(context.getString(USERNAME));
            }
            if (StringUtils.isNotBlank(context.getString(PASSWORD))) {
                password = (String) dwMap.get(context.getString(PASSWORD));
            }
        }
        if (StringUtils.isNotBlank(context.getString(TABLE_TYPE))) {
            tableType = context.getString(TABLE_TYPE);
        }
        if (StringUtils.isNotBlank(context.getString(SQL))) {
            sql = context.getString(SQL);
        }

        //对事件进行解析
        if (StringUtils.isNotBlank(context.getString(CLASS_NAME))) {
            className = context.getString(CLASS_NAME);
        }

        if (StringUtils.isNotBlank(context.getString(MONITORURL))) {
            monitorUrl = context.getString(MONITORURL);
        }

        //多线程参数
        if (StringUtils.isNotBlank(context.getString("isOpenThread"))) {
            this.isOpenThread = context.getBoolean("isOpenThread");
        }
        if (StringUtils.isNotBlank(context.getString("threadNUM"))) {
            this.DEFAULT_CLIENT_THREAD_NUM = context.getInteger("threadNUM");
        }


        tableName = context.getString(HBaseSinkConfigurationConstants.CONFIG_TABLE);
        String cf = context.getString(
                HBaseSinkConfigurationConstants.CONFIG_COLUMN_FAMILY);
        batchSize = context.getLong(
                HBaseSinkConfigurationConstants.CONFIG_BATCHSIZE, new Long(100));
        serializerContext = new Context();
        //If not specified, will use HBase defaults.
        eventSerializerType = context.getString(
                HBaseSinkConfigurationConstants.CONFIG_SERIALIZER);
        Preconditions.checkNotNull(tableName,
                "Table name cannot be empty, please specify in configuration file");
        Preconditions.checkNotNull(cf,
                "Column family cannot be empty, please specify in configuration file");
        //Check foe event serializer, if null set event serializer type
        if (eventSerializerType == null || eventSerializerType.isEmpty()) {
            eventSerializerType =
                    "org.apache.flume.sink.hbase.SimpleHbaseEventSerializer";
            logger.info("No serializer defined, Will use default");
        }
        serializerContext.putAll(context.getSubProperties(
                HBaseSinkConfigurationConstants.CONFIG_SERIALIZER_PREFIX));
        columnFamily = cf.getBytes(Charsets.UTF_8);
        try {
            Class<? extends HbaseEventSerializer> clazz =
                    (Class<? extends HbaseEventSerializer>)
                            Class.forName(eventSerializerType);
            serializer = clazz.newInstance();
            serializer.configure(serializerContext);
        } catch (Exception e) {
            logger.error("Could not instantiate event serializer.", e);
            Throwables.propagate(e);
        }
        kerberosKeytab = context.getString(HBaseSinkConfigurationConstants.CONFIG_KEYTAB, "");
        kerberosPrincipal = context.getString(HBaseSinkConfigurationConstants.CONFIG_PRINCIPAL, "");

        enableWal = context.getBoolean(HBaseSinkConfigurationConstants
                .CONFIG_ENABLE_WAL, HBaseSinkConfigurationConstants.DEFAULT_ENABLE_WAL);
        logger.info("The write to WAL option is set to: " + String.valueOf(enableWal));
        if (!enableWal) {
            logger.warn("HBase Sink's enableWal configuration is set to false. All " +
                    "writes to HBase will have WAL disabled, and any data in the " +
                    "memstore of this region in the Region Server could be lost!");
        }

        batchIncrements = context.getBoolean(
                HBaseSinkConfigurationConstants.CONFIG_COALESCE_INCREMENTS,
                HBaseSinkConfigurationConstants.DEFAULT_COALESCE_INCREMENTS);

        if (batchIncrements) {
            logger.info("Increment coalescing is enabled. Increments will be " +
                    "buffered.");
            refGetFamilyMap = reflectLookupGetFamilyMap();
        }
        String zkQuorum = (StringUtils.isBlank((hbasezk == null) ? "" : hbasezk.get("Url").toString())) ? context.getString("zookeeperQuorum") : (String) hbasezk.get("Url");
        Integer port = null;
        /**
         * HBase allows multiple nodes in the quorum, but all need to use the
         * same client port. So get the nodes in host:port format,
         * and ignore the ports for all nodes except the first one. If no port is
         * specified, use default.
         */
        if (zkQuorum != null && !zkQuorum.isEmpty()) {
            StringBuilder zkBuilder = new StringBuilder();
            logger.info("===========zkQuorum============ " + zkQuorum);
            String[] zkHosts = zkQuorum.split(",");
            int length = zkHosts.length;
            for (int i = 0; i < length; i++) {
                String[] zkHostAndPort = zkHosts[i].split(":");
                zkBuilder.append(zkHostAndPort[0].trim());
                if (i != length - 1) {
                    zkBuilder.append(",");
                } else {
                    zkQuorum = zkBuilder.toString();
                }
                if (zkHostAndPort[1] == null) {
                    throw new FlumeException("Expected client port for the ZK node!");
                }
                if (port == null) {
                    port = Integer.parseInt(zkHostAndPort[1].trim());
                } else if (!port.equals(Integer.parseInt(zkHostAndPort[1].trim()))) {
                    throw new FlumeException("All Zookeeper nodes in the quorum must " +
                            "use the same client port.");
                }
            }
            if (port == null) {
                port = HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT;
            }
            this.config.set(HConstants.ZOOKEEPER_QUORUM, zkQuorum);
            this.config.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, port);
        }
        String hbaseZnode = context.getString(
                HBaseSinkConfigurationConstants.ZK_ZNODE_PARENT);
        System.out.println("===hbaseZnode==" + hbaseZnode);
        if (hbaseZnode != null && !hbaseZnode.isEmpty()) {
            this.config.set(HConstants.ZOOKEEPER_ZNODE_PARENT, hbaseZnode);
        }
        sinkCounter = new SinkCounter(this.getName());

    }

    public Configuration getConfig() {
        return config;
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = Status.READY;
        Channel channel = getChannel();
        Transaction txn = channel.getTransaction();
        List<Row> actions = new LinkedList<Row>();
        List<Increment> incs = new LinkedList<Increment>();
        try {
            txn.begin();
            if (serializer instanceof BatchAware) {
                ((BatchAware) serializer).onBatchStart();
            }

            long i = 0;
            for (; i < batchSize; i++) {
                Event event = channel.take();
                if (event == null) {
                    if (i == 0) {
                        status = Status.BACKOFF;
                        sinkCounter.incrementBatchEmptyCount();
                    } else {
                        sinkCounter.incrementBatchUnderflowCount();
                    }
                    break;
                } else {
                    cls = Class.forName(className);
                    fileFormat = (FileFormat) cls.newInstance();
                    msg = fileFormat.msgToFlumeSinks(event.getBody());

                    if (msg == null) {
                        continue;
                    }
                    event.setBody(msg);

                    if (isOpenThread) {
                        bufferQueue.put(event);
                    } else {
//                        if (msg != null) {
//                        event.setBody(msg);
                        serializer.initialize(event, columnFamily);
                        actions.addAll(serializer.getActions());
                        incs.addAll(serializer.getIncrements());
//                        }
                    }

                }
            }

            if (i == batchSize) {
                sinkCounter.incrementBatchCompleteCount();
            }
            sinkCounter.addToEventDrainAttemptCount(i);

            if (!isOpenThread) {
                putEventsAndCommit(actions, incs, txn);
            } else {
                txn.commit();
            }

        } catch (Throwable e) {
            try {
                txn.rollback();
            } catch (Exception e2) {
                logger.error("Exception in rollback. Rollback might not have been " +
                        "successful.", e2);
            }
            logger.error("Failed to commit transaction." +
                    "Transaction rolled back.", e);
            if (e instanceof Error || e instanceof RuntimeException) {
                logger.error("Failed to commit transaction." +
                        "Transaction rolled back.", e);
                Throwables.propagate(e);
            } else {
                logger.error("Failed to commit transaction." +
                        "Transaction rolled back.", e);
                throw new EventDeliveryException("Failed to commit transaction." +
                        "Transaction rolled back.", e);
            }
        } finally {
            txn.close();
        }
        return status;
    }

    private void putEventsAndCommit(final List<Row> actions,
                                    final List<Increment> incs, Transaction txn) throws Exception {

        runPrivileged(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {

/*        for (Row r : actions) {
          if (r instanceof Put) {
            ((Put) r).setWriteToWAL(enableWal);
          }
          // Newer versions of HBase - Increment implements Row.
          if (r instanceof Increment) {
            ((Increment) r).setWriteToWAL(enableWal);
          }
        }*/
                try {
                    Long startTime = System.currentTimeMillis();
                    table.batch(actions);
                    Long endTime = System.currentTimeMillis();
                    logger.info("===tableName:" + table + ",count:{}, 耗时: {} ms", actions.size(), (endTime - startTime));
                } catch (Exception e) {
                    try {
                        table.batch(actions);
                    } catch (Exception e1) {
                        try {
                            MonitorInfluxdb.SendMonitorClient(MonitorMsg.buildMsgInfo("flume", System.currentTimeMillis(), 2, tableName, "FLUME"), monitorUrl);
                        } catch (Exception e2) {
                            logger.error(" error : MonitorInfluxdb:" + e2);
                        }
                        logger.error("hbase tableName:" + table + " error msg:" + e);
                        logger.error(actions + "hbase put  failed!" + table + "error msg:" + e1);
                    }
                }

                return null;
            }
        });

        runPrivileged(new PrivilegedExceptionAction<Void>() {
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
        });

        txn.commit();
        sinkCounter.addToEventDrainSuccessCount(actions.size());
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

    /**
     * The method getFamilyMap() is no longer available in Hbase 0.96.
     * We must use reflection to determine which version we may use.
     */
    @VisibleForTesting
    static Method reflectLookupGetFamilyMap() {
        Method m = null;
        String[] methodNames = {"getFamilyMapOfLongs", "getFamilyMap"};
        for (String methodName : methodNames) {
            try {
                m = Increment.class.getMethod(methodName);
                if (m != null && m.getReturnType().equals(Map.class)) {
                    logger.debug("Using Increment.{} for coalesce", methodName);
                    break;
                }
            } catch (NoSuchMethodException e) {
                logger.debug("Increment.{} does not exist. Exception follows.",
                        methodName, e);
            } catch (SecurityException e) {
                logger.debug("No access to Increment.{}; Exception follows.",
                        methodName, e);
            }
        }
        if (m == null) {
            throw new UnsupportedOperationException(
                    "Cannot find Increment.getFamilyMap()");
        }
        return m;
    }

    @SuppressWarnings("unchecked")
    private Map<byte[], NavigableMap<byte[], Long>> getFamilyMap(Increment inc) {
        Preconditions.checkNotNull(refGetFamilyMap,
                "Increment.getFamilymap() not found");
        Preconditions.checkNotNull(inc, "Increment required");
        Map<byte[], NavigableMap<byte[], Long>> familyMap = null;
        try {
            Object familyObj = refGetFamilyMap.invoke(inc);
            familyMap = (Map<byte[], NavigableMap<byte[], Long>>) familyObj;
        } catch (IllegalAccessException e) {
            logger.warn("Unexpected error calling getFamilyMap()", e);
            Throwables.propagate(e);
        } catch (InvocationTargetException e) {
            logger.warn("Unexpected error calling getFamilyMap()", e);
            Throwables.propagate(e);
        }
        return familyMap;
    }

    /**
     * Perform "compression" on the given set of increments so that Flume sends
     * the minimum possible number of RPC operations to HBase per batch.
     *
     * @param incs Input: Increment objects to coalesce.
     * @return List of new Increment objects after coalescing the unique counts.
     */
    private List<Increment> coalesceIncrements(Iterable<Increment> incs) {
        Preconditions.checkNotNull(incs, "List of Increments must not be null");
        // Aggregate all of the increment row/family/column counts.
        // The nested map is keyed like this: {row, family, qualifier} => count.
        Map<byte[], Map<byte[], NavigableMap<byte[], Long>>> counters =
                Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
        for (Increment inc : incs) {
            byte[] row = inc.getRow();
            Map<byte[], NavigableMap<byte[], Long>> families = getFamilyMap(inc);
            for (Map.Entry<byte[], NavigableMap<byte[], Long>> familyEntry : families.entrySet()) {
                byte[] family = familyEntry.getKey();
                NavigableMap<byte[], Long> qualifiers = familyEntry.getValue();
                for (Map.Entry<byte[], Long> qualifierEntry : qualifiers.entrySet()) {
                    byte[] qualifier = qualifierEntry.getKey();
                    Long count = qualifierEntry.getValue();
                    incrementCounter(counters, row, family, qualifier, count);
                }
            }
        }

        // Reconstruct list of Increments per unique row/family/qualifier.
        List<Increment> coalesced = Lists.newLinkedList();
        for (Map.Entry<byte[], Map<byte[], NavigableMap<byte[], Long>>> rowEntry : counters.entrySet()) {
            byte[] row = rowEntry.getKey();
            Map<byte[], NavigableMap<byte[], Long>> families = rowEntry.getValue();
            Increment inc = new Increment(row);
            for (Map.Entry<byte[], NavigableMap<byte[], Long>> familyEntry : families.entrySet()) {
                byte[] family = familyEntry.getKey();
                NavigableMap<byte[], Long> qualifiers = familyEntry.getValue();
                for (Map.Entry<byte[], Long> qualifierEntry : qualifiers.entrySet()) {
                    byte[] qualifier = qualifierEntry.getKey();
                    long count = qualifierEntry.getValue();
                    inc.addColumn(family, qualifier, count);
                }
            }
            coalesced.add(inc);
        }

        return coalesced;
    }

    /**
     * Helper function for {@link #coalesceIncrements} to increment a counter
     * value in the passed data structure.
     *
     * @param counters  Nested data structure containing the counters.
     * @param row       Row key to increment.
     * @param family    Column family to increment.
     * @param qualifier Column qualifier to increment.
     * @param count     Amount to increment by.
     */
    private void incrementCounter(
            Map<byte[], Map<byte[], NavigableMap<byte[], Long>>> counters,
            byte[] row, byte[] family, byte[] qualifier, Long count) {

        Map<byte[], NavigableMap<byte[], Long>> families = counters.get(row);
        if (families == null) {
            families = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
            counters.put(row, families);
        }

        NavigableMap<byte[], Long> qualifiers = families.get(family);
        if (qualifiers == null) {
            qualifiers = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
            families.put(family, qualifiers);
        }

        Long existingValue = qualifiers.get(qualifier);
        if (existingValue == null) {
            qualifiers.put(qualifier, count);
        } else {
            qualifiers.put(qualifier, existingValue + count);
        }
    }

    @VisibleForTesting
    @InterfaceAudience.Private
    HbaseEventSerializer getSerializer() {
        return serializer;
    }

    @VisibleForTesting
    @InterfaceAudience.Private
    interface DebugIncrementsCallback {
        public void onAfterCoalesce(Iterable<Increment> increments);
    }
}
