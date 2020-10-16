package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.StockBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.function.KeySelectorByCode;
import cn.itcast.function.MinStockWindowFunction;
import cn.itcast.inter.ProcessDataInterface;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Properties;

public class StockQuotMinTask implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {
        /**
         * 开发步骤：
         * 1.定义侧边流
         * 2.数据分组
         * 3.划分时间窗口
         * 4.分时数据处理（新建分时窗口函数）
         * 5.数据分流
         * 6.数据分流转换
         * 7.分表存储(写入kafka)
         */
        //1.定义侧边流
        OutputTag<StockBean> sseOutPutTag = new OutputTag<>("sseStockMin", TypeInformation.of(StockBean.class));
        //2.数据分组
        SingleOutputStreamOperator<StockBean> result = waterData
                .keyBy(new KeySelectorByCode())
                //3.划分时间窗口
                .timeWindow(Time.seconds(60))
                //4.分时数据处理（新建分时窗口函数）
                .apply(new MinStockWindowFunction())
                //5.数据分流
                .process(new ProcessFunction<StockBean, StockBean>() {
                    @Override
                    public void processElement(StockBean stockBean, Context context, Collector<StockBean> collector) throws Exception {
                        if ("szse".equals(stockBean.getSource())) {
                            collector.collect(stockBean);
                        } else {
                            context.output(sseOutPutTag, stockBean);
                        }
                    }
                });
        //深股数据
        SingleOutputStreamOperator<String> szseStr = result.keyBy(new KeySelector<StockBean, String>() {
            @Override
            public String getKey(StockBean stockBean) throws Exception {
                return stockBean.getSecCode();
            }
        }).map(new MapFunction<StockBean, String>() {
            @Override
            public String map(StockBean stockBean) throws Exception {
                return JSON.toJSONString(stockBean);
            }
        });
        //上股数据
        SingleOutputStreamOperator<String> sseStr = result.getSideOutput(sseOutPutTag)
                .keyBy(new KeySelector<StockBean, String>() {
                    @Override
                    public String getKey(StockBean stockBean) throws Exception {
                        return stockBean.getSecCode();
                    }
                }).map(new MapFunction<StockBean, String>() {
                    @Override
                    public String map(StockBean stockBean) throws Exception {
                        return JSON.toJSONString(stockBean);
                    }
                });
            //7.分表存储(写入kafka)
            //新建kafka生产者对象

        szseStr.print("szse:");
        sseStr.print("sse");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", QuotConfig.config.getProperty("bootstrap.servers"));

        FlinkKafkaProducer011<String> sseKafkaProducer = new FlinkKafkaProducer011<>(QuotConfig.config.getProperty("sse.stock.topic"), new SimpleStringSchema(), properties);
        FlinkKafkaProducer011<String> szseKafkaProducer = new FlinkKafkaProducer011<>(QuotConfig.config.getProperty("szse.stock.topic"), new SimpleStringSchema(), properties);
        //写入kafka
        szseStr.addSink(szseKafkaProducer);
        sseStr.addSink(sseKafkaProducer);


    }
}
