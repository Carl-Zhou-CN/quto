package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.IndexBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.function.KeySelectorByCode;
import cn.itcast.function.MinIndexWindowFunction;
import cn.itcast.inter.ProcessDataInterface;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Properties;

//分时行情数据
public class IndexMinTask implements ProcessDataInterface {
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
        OutputTag<IndexBean> sseOutputTag = new OutputTag<>("sseIndexMin", TypeInformation.of(IndexBean.class));
        //2.数据分组
        SingleOutputStreamOperator<IndexBean> processData = waterData
                //2.数据分组
                .keyBy(new KeySelectorByCode())
                //3.划分时间窗口
                .timeWindow(Time.seconds(60))
                //4.分时数据处理（新建分时窗口函数）
                .apply(new MinIndexWindowFunction())
                //数据分流
                .process(new ProcessFunction<IndexBean, IndexBean>() {
                    @Override
                    public void processElement(IndexBean indexBean, Context context, Collector<IndexBean> collector) throws Exception {
                        if ("szse".equals(indexBean.getSource())) {
                            collector.collect(indexBean);
                        } else {
                            context.output(sseOutputTag, indexBean);
                        }
                    }
                });

        //7.分表存储(写入kafka)
        SingleOutputStreamOperator<String> szseStr = processData.map(new MapFunction<IndexBean, String>() {
            @Override
            public String map(IndexBean indexBean) throws Exception {
                return JSON.toJSONString(indexBean);
            }
        });
        SingleOutputStreamOperator<String> sseStr = processData
                .getSideOutput(sseOutputTag)
                .map(new MapFunction<IndexBean, String>() {
                    @Override
                    public String map(IndexBean indexBean) throws Exception {
                        return JSON.toJSONString(indexBean);
                    }
                });

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", QuotConfig.config.getProperty("bootstrap.servers"));

        FlinkKafkaProducer011<String> szseKafkaPro = new FlinkKafkaProducer011<>(QuotConfig.config.getProperty("szse.index.topic"), new SimpleStringSchema(), properties);
        FlinkKafkaProducer011<String> sseKafkaPro = new FlinkKafkaProducer011<>(QuotConfig.config.getProperty("sse.index.topic"), new SimpleStringSchema(), properties);

        szseStr.addSink(szseKafkaPro);
        sseStr.addSink(sseKafkaPro);
    }
}
