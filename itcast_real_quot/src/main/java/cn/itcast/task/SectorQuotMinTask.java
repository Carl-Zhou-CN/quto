package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.SectorBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.function.MinStockWindowFunction;
import cn.itcast.function.SectorWindowFunction;
import cn.itcast.inter.ProcessDataInterface;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.table.api.scala.map;

import java.util.Properties;

public class SectorQuotMinTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {
        /**
         * 1.数据分组
         * 2.划分个股时间窗口
         * 3.个股分时数据处理
         * 4.划分板块时间窗口
         * 5.板块分时数据处理
         * 6.数据转换成字符串
         * 7.数据写入kafka
         */
        waterData
                .keyBy(CleanBean :: getSecCode)
                .timeWindow(Time.seconds(60L))
                .apply(new MinStockWindowFunction())
                .timeWindowAll(Time.seconds(60L))
                .apply(new SectorWindowFunction())
                .keyBy(SectorBean :: getSectorCode)
                .map(new MapFunction<SectorBean, String>() {
                    @Override
                    public String map(SectorBean sectorBean) throws Exception {
                        return JSON.toJSONString(sectorBean);
                    }
                });
        //7.数据写入kafka
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", QuotConfig.config.getProperty("bootstrap.servers"));
        FlinkKafkaProducer011 kafkaProducer = new FlinkKafkaProducer011<String>(QuotConfig.config.getProperty("sse.sector.topic"),
                new SimpleStringSchema(), properties);

        waterData.addSink(kafkaProducer);
    }
}
