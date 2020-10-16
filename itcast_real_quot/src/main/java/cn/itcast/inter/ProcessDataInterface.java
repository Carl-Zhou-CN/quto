package cn.itcast.inter;

import cn.itcast.bean.CleanBean;
import org.apache.flink.streaming.api.datastream.DataStream;


public interface ProcessDataInterface {
    void process(DataStream<CleanBean> waterData);
}
