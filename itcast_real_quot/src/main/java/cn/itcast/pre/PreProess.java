package cn.itcast.pre;

import cn.itcast.avro.AvroDeserializeSchema;
import cn.itcast.avro.SseAvro;
import cn.itcast.avro.SzseAvro;
import cn.itcast.bean.CleanBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.job.IndexStream;
import cn.itcast.map.TMap;
import cn.itcast.util.QuotUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import javax.annotation.Nullable;
import java.util.Properties;

public class PreProess {

    public  SingleOutputStreamOperator<CleanBean> getDataStream(StreamExecutionEnvironment env,Class clazz){
        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", QuotConfig.config.getProperty("bootstrap.servers"));
        properties.setProperty("group.id", QuotConfig.config.getProperty("group.id"));

        FlinkKafkaConsumer011<SseAvro> sseKafka = new FlinkKafkaConsumer011<SseAvro>(QuotConfig.config.getProperty("sse.stock.topic"), new AvroDeserializeSchema(QuotConfig.config.getProperty("sse.stock.topic")), properties);
        sseKafka.setStartFromEarliest();
        DataStreamSource<SseAvro> sseSource = env.addSource(sseKafka);

        //sseSource.print("沪市 :");


        FlinkKafkaConsumer011<SzseAvro> szseKafka = new FlinkKafkaConsumer011<SzseAvro>(QuotConfig.config.getProperty("szse.stock.topic"), new AvroDeserializeSchema(QuotConfig.config.getProperty("szse.stock.topic")), properties);
        szseKafka.setStartFromLatest();
        DataStreamSource<SzseAvro> szseSource = env.addSource(szseKafka);
        //szseSource.print("深市:");
        SingleOutputStreamOperator<SseAvro> sseFilterData  = sseSource.filter(new FilterFunction<SseAvro>() {
            @Override
            public boolean filter(SseAvro sseAvro) throws Exception {
                return QuotUtil.checkTime(sseAvro) && QuotUtil.checkData(sseAvro);
            }
        });

        SingleOutputStreamOperator<SzseAvro> szseFilterData  = szseSource.filter(new FilterFunction<SzseAvro>() {
            @Override
            public boolean filter(SzseAvro szseAvro) throws Exception {
                return QuotUtil.checkTime(szseAvro) && QuotUtil.checkData(szseAvro);
            }
        });

        SingleOutputStreamOperator<CleanBean> sseMapData = sseFilterData.map(new TMap<SseAvro>());
        SingleOutputStreamOperator<CleanBean> szseMapData = szseFilterData.map(new TMap<SzseAvro>());

        DataStream<CleanBean> unionData = sseMapData.union(szseMapData);

        SingleOutputStreamOperator<CleanBean> filterData = unionData.filter(new FilterFunction<CleanBean>() {
            @Override
            public boolean filter(CleanBean cleanBean) throws Exception {
                if (clazz.getSimpleName().contains("Index")){
                    return QuotUtil.isIndex(cleanBean);
                }else {
                    return QuotUtil.isStock(cleanBean);
                }
            }
        });
        SingleOutputStreamOperator<CleanBean> waterData = filterData.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<CleanBean>() {
            //延时时间
            long delayTime = Long.parseLong(QuotConfig.config.getProperty("delay.time"));
            //当前最大时间时间
            long currentTimestamp = 0L;

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentTimestamp - delayTime - 1);
            }

            @Override
            public long extractTimestamp(CleanBean cleanBean, long l) {
                currentTimestamp = Math.max(currentTimestamp, cleanBean.getEventTime());
                return cleanBean.getEventTime();
            }
        });
        return waterData;
    }
}
