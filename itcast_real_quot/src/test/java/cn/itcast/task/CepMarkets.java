package cn.itcast.task;

import cn.itcast.bean.Product;
import cn.itcast.util.RedisUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import redis.clients.jedis.JedisCluster;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class CepMarkets {
    public static void main(String[] args) throws Exception {
        /**
         * 1.获取流处理执行环境
         * 2.设置事件时间、并行度
         * 3.整合kafka
         * 4.数据转换
         * 5.获取bean数据，设置事件时间
         * 6.定义匹配模式，设置时间长度
         * 7.匹配模式（分组）
         * 8.查询告警数据
         */
        //1.获取流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.设置事件时间、并行度
        env.setParallelism(1);
        //3.整合kafka
        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "node01:9092"); //broker地址
        properties.setProperty("group.id", "cep"); //消费组
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty("auto.commit.interval.ms", "5000");

        FlinkKafkaConsumer011<String> cepKafkaCsr = new FlinkKafkaConsumer011<>("cep", new SimpleStringSchema(), properties);

        cepKafkaCsr.setStartFromEarliest();

        DataStreamSource<String> kafkaSource = env.addSource(cepKafkaCsr);
        //数据转化
        SingleOutputStreamOperator<Product> mapData = kafkaSource.map(new MapFunction<String, Product>() {
            @Override
            public Product map(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                return new Product(
                        jsonObject.getLong("goodsId"),
                        jsonObject.getDouble("goodsPrice"),
                        jsonObject.getString("goodsName"),
                        jsonObject.getString("alias"),
                        jsonObject.getLong("orderTime"),
                        false
                );
            }
        });


        //获取bean数据，设置事件时间
        SingleOutputStreamOperator<Product> waterData = mapData.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Product>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(Product element) {
                return element.getOrderTime();
            }
        })
                .keyBy(Product::getGoodsId)
                .process(new KeyedProcessFunction<Long, Product, Product>() {
                    Map<String, String> map = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        JedisCluster jedisCluster = RedisUtil.getJedisCluster();
                        map = jedisCluster.hgetAll("product");
                    }

                    @Override
                    public void processElement(Product value, Context ctx, Collector<Product> out) throws Exception {
                        long priceAlert = Long.parseLong(map.get(value.getGoodsName()));
                        if (value.getGoodsPrice() > priceAlert) {
                            value.setStatus(true);
                        }
                        out.collect(value);
                    }
                });

            //在此，我们假定如果商品售价在1分钟之内有连续两次超过预定商品价格阀值就发送告警信息。

        Pattern<Product, Product> pattern = Pattern.<Product>begin("begin")
                .where(new SimpleCondition<Product>() {
                    @Override
                    public boolean filter(Product product) throws Exception {
                        return product.getStatus() == true;
                    }
                })
                .next("next")
                .where(new SimpleCondition<Product>() {
                    @Override
                    public boolean filter(Product product) throws Exception {
                        return product.getStatus() == true;
                    }
                })
                .within(Time.minutes(1L));

        PatternStream<Product> cep = CEP.pattern(waterData.keyBy(Product::getGoodsId), pattern);

        cep.select(new PatternSelectFunction<Product, Object>() {
            @Override
            public Object select(Map<String, List<Product>> map) throws Exception {
                List<Product> result = map.get("next");
                return result;
            }
        }).print();

        env.execute();
    }
}
