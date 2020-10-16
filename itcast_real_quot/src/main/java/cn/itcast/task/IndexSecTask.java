package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.function.IndexPutHbaseWindowFunction;
import cn.itcast.function.KeySelectorByCode;
import cn.itcast.function.SecIndexWindowFunction;
import cn.itcast.inter.ProcessDataInterface;
import cn.itcast.sink.SinkHbase;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;

//指数秒级行情处理
public class IndexSecTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {
        /**
         *
         * 开发步骤：
         * 1.数据分组
         * 2.划分时间窗口
         * 3.秒级数据处理（新建数据写入Bean和秒级窗口函数）
         * 4.数据写入操作
         * 封装ListPuts
         * 数据写入
         *
         */

        waterData
                //1.数据分组
                .keyBy(new KeySelectorByCode())
                //2.划分时间窗口
                .timeWindow(Time.seconds(5))
                //3.秒级数据处理（新建数据写入Bean和秒级窗口函数）
                .apply(new SecIndexWindowFunction())
                //4.数据写入操作
                .timeWindowAll(Time.seconds(5))
                .apply(new IndexPutHbaseWindowFunction())
                //数据写入
                .addSink(new SinkHbase(QuotConfig.config.getProperty("index.hbase.table.name")));
    }
}
