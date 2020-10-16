package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.StockBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.function.KeySelectorByCode;
import cn.itcast.function.MinStockWindowFunction02;
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

//分时行情
public class StockQuotMinTask02 implements ProcessDataInterface {
    @Override
    public void process(DataStream<CleanBean> waterData) {
        //定义侧边流
        //数据分组
        //划分窗口
        //新建窗口处理函数,求分时成交量和分时成交金额,封装StockBean对象
        //数据分流
        //分别写入Kafka
        OutputTag<StockBean> sseOutputTag = new OutputTag<>("sseOutputTag", TypeInformation.of(StockBean.class));

        SingleOutputStreamOperator<StockBean> processData = waterData.keyBy(new KeySelectorByCode())
                .timeWindow(Time.seconds(60))
                .apply(new MinStockWindowFunction02())
                .process(new ProcessFunction<StockBean, StockBean>() {
                    @Override
                    public void processElement(StockBean value, Context ctx, Collector<StockBean> out) throws Exception {
                        if ("szse".equals(value.getSecCode())) {
                            out.collect(value);
                        } else {
                            ctx.output(sseOutputTag, value);
                        }
                    }
                });
        //深市数据
        SingleOutputStreamOperator<String> szseStr = processData.keyBy(new KeySelector<StockBean, String>() {
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
        SingleOutputStreamOperator<String> sseStr = processData.getSideOutput(sseOutputTag)
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
        //分表写入kafka
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", QuotConfig.config.getProperty("bootstrap.servers"));


        FlinkKafkaProducer011<String> sseKafkaProducer = new FlinkKafkaProducer011<>(QuotConfig.config.getProperty("sse.stock.topic"), new SimpleStringSchema(), properties);
        FlinkKafkaProducer011<String> szseKafkaProducer = new FlinkKafkaProducer011<>(QuotConfig.config.getProperty("szse.stock.topic"), new SimpleStringSchema(), properties);
        //写入kafka
        szseStr.addSink(szseKafkaProducer);
        sseStr.addSink(sseKafkaProducer);

    }
}
