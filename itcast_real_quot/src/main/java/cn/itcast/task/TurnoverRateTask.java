package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.inter.ProcessDataCepInterface;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TurnoverRateTask implements ProcessDataCepInterface {
    @Override
    public void process(DataStream<CleanBean> waterData, StreamExecutionEnvironment env) {

    }
}
