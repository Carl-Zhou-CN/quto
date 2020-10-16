package cn.itcast.job;

import cn.itcast.bean.CleanBean;
import cn.itcast.pre.PreProess;
import cn.itcast.task.SectorKlineTask;
import cn.itcast.task.SectorQuotMinHdfsTask;
import cn.itcast.task.SectorQuotMinTask;
import cn.itcast.task.SectorQuotSecTask;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SectorStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.setParallelism(1);

        PreProess preProess = new PreProess();
        //获取增加水位后的个股数据:
        SingleOutputStreamOperator<CleanBean> waterData = preProess.getDataStream(env, SectorStream.class);

        //板块秒级行情
        new SectorQuotSecTask().process(waterData);
        //板块分时行情
        new SectorQuotMinTask().process(waterData);
        //板块分时备份
        new SectorQuotMinHdfsTask().process(waterData);
        //板块K线
        new SectorKlineTask().process(waterData);
        env.execute();
    }
}
