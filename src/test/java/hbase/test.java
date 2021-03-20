package hbase;

import cn.teld.bdp.flume.monitor.MonitorInfluxdb;
import cn.teld.bdp.flume.monitor.MonitorMsg;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.sink.hbase.HBaseSink;

public class test {
    public static void main(String[] args) throws EventDeliveryException {
//        MonitorInfluxdb.SendMonitorClient(MonitorMsg.buildMsgInfo("flume",System.currentTimeMillis(),2,"ss","FLUME"),"dds");
        new HBaseSink().process();
    }

}
