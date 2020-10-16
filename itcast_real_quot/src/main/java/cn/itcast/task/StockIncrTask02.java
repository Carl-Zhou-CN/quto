package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.inter.ProcessDataInterface;
import org.apache.flink.streaming.api.datastream.DataStream;
//涨跌幅行情
public class StockIncrTask02 implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {

    }
}
