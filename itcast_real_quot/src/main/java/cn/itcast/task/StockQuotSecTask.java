package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.function.KeySelectorByCode;
import cn.itcast.function.SecStockWindowFunction;
import cn.itcast.function.StockHbasePutsWindowFuntion;
import cn.itcast.inter.ProcessDataInterface;
import cn.itcast.sink.SinkHbase;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;


public class StockQuotSecTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {
        /**
         * 1.数据分组
         * 2.划分时间窗口
         * 3.新建个股数据写入bean对象
         * 4.秒级窗口函数业务处理
         * 5.数据写入操作
         *  封装ListPuts
         *  数据写入
         */
        waterData
                //1.数据分组
                .keyBy(new KeySelectorByCode())
                //2.划分时间窗口
                .timeWindow(Time.seconds(5))
                //3.新建个股数据写入bean对象
                .apply(new SecStockWindowFunction())
                //4.秒级窗口函数业务处理
                .timeWindowAll(Time.seconds(5))
                //5.数据写入操作
                .apply(new StockHbasePutsWindowFuntion())
                .addSink(new SinkHbase(QuotConfig.config.getProperty("stock.hbase.table.name")));
    }
}
