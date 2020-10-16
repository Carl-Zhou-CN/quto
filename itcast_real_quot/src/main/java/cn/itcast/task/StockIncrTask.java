package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.StockIncrBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.function.KeySelectorByCode;
import cn.itcast.function.StockMinIncrWindowFunction;
import cn.itcast.inter.ProcessDataInterface;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;

public class StockIncrTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {
        /**
         * 开发步骤：
         * 1.数据分组
         * 2.划分时间窗口
         * 3.创建bean对象
         * 4.分时数据处理（新建分时窗口函数）
         * 5.数据转换成字符串
         * 6.数据存储(单表)
         */

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", QuotConfig.config.getProperty("bootstrap.servers"));

        FlinkKafkaProducer011<String> kafkaPre = new FlinkKafkaProducer011<>(QuotConfig.config.getProperty("stock.increase.topic"), new SimpleStringSchema(), properties);


        waterData
                .keyBy(new KeySelectorByCode())
                .timeWindow(Time.seconds(60))
                .apply(new StockMinIncrWindowFunction())
                .map(new MapFunction<StockIncrBean, String>() {
                    @Override
                    public String map(StockIncrBean stockIncrBean) throws Exception {
                        return JSON.toJSONString(stockIncrBean);
                    }
                })
                .addSink(kafkaPre);

    }
}
