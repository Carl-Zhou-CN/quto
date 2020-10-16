package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.function.MinStockWindowFunction;
import cn.itcast.function.SectorHbasePutsWindowFunction;
import cn.itcast.function.SectorWindowFunction02;
import cn.itcast.inter.ProcessDataInterface;
import cn.itcast.sink.SinkHbase;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;

//板块秒级行情
public class SectorQuotSecTask02 implements ProcessDataInterface {

    @Override
    public void process(DataStream<CleanBean> waterData) {
        /**
         * 1.数据分组
         * 2.划分时间窗口
         * 3.个股数据处理
         * 4.划分时间窗口
         * 5.秒级数据处理（新建数据写入样例类和秒级窗口函数）
         * 6.数据写入操作
         * * 封装ListPuts
         * * 数据写入
         */
        waterData
                .keyBy(CleanBean :: getSecCode)
                .timeWindow(Time.seconds(5L))
                .apply(new MinStockWindowFunction())
                .timeWindowAll(Time.seconds(5L))
                .apply(new SectorWindowFunction02())
                .timeWindowAll(Time.seconds(5L))
                .apply(new SectorHbasePutsWindowFunction())
                .addSink(new SinkHbase(QuotConfig.config.getProperty("sector.hbase.table.name")));
    }
}
