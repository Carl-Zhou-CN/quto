package cn.itcast.task;

import cn.itcast.task.source.*;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.jasper.tagplugins.jstl.core.Out;

import java.util.List;
import java.util.Map;

public class RackWarn {
    public static void main(String[] args) throws Exception {
        /**
         * 1.获取流处理执行环境
         * 2.设置事件时间
         * 3.加载数据源，接收监视数据,设置提取时间
         * 4.定义匹配模式，设置预警匹配规则，警告：10s内连续两次超过阀值
         * 5.生成匹配模式流（分组）
         * 6.数据处理,生成警告数据
         * 7.二次定义匹配模式，告警：20s内连续两次匹配警告
         * 8.二次生成匹配模式流（分组）
         * 9.数据处理生成告警信息flatSelect，返回类型
         * 10.数据打印(警告和告警)
         * 11.触发执行
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<MonitoringEvent> source = env.addSource(new MonitoringEventSource()).assignTimestampsAndWatermarks(new IngestionTimeExtractor<>());

        Pattern<MonitoringEvent, TemperatureEvent> pattern = Pattern.<MonitoringEvent>begin("begin").subtype(TemperatureEvent.class)
                .where(new SimpleCondition<TemperatureEvent>() {
                    @Override
                    public boolean filter(TemperatureEvent temperatureEvent) throws Exception {
                        return temperatureEvent.getTemperature() > 100;
                    }
                }).next("next").subtype(TemperatureEvent.class)
                .where(new SimpleCondition<TemperatureEvent>() {
                    @Override
                    public boolean filter(TemperatureEvent temperatureEvent) throws Exception {
                        return temperatureEvent.getTemperature() > 100;
                    }
                }).within(Time.seconds(10L));

        PatternStream<MonitoringEvent> cep = CEP.pattern(source.keyBy(MonitoringEvent ::getRackID), pattern);

        SingleOutputStreamOperator<TemperatureWarning> warnData  = cep.select(new PatternSelectFunction<MonitoringEvent, TemperatureWarning>() {
            @Override
            public TemperatureWarning select(Map<String, List<MonitoringEvent>> map) throws Exception {
                TemperatureEvent begin = (TemperatureEvent) map.get("begin").get(0);
                TemperatureEvent next = (TemperatureEvent) map.get("next").get(0);
                return new TemperatureWarning(begin.getRackID(), (begin.getTemperature() + next.getTemperature()) / 2);
            }
        });

        warnData.print("警告");
        //二次定义匹配模式
        Pattern<TemperatureWarning, TemperatureWarning> pattern1 = pattern.<TemperatureWarning>begin("begin")
                .next("next")
                .within(Time.seconds(20));

        PatternStream<TemperatureWarning> cep2 = CEP.pattern(warnData.keyBy(TemperatureWarning::getRackID), pattern1);

        SingleOutputStreamOperator<TemperatureAlert> result = cep2.flatSelect(new PatternFlatSelectFunction<TemperatureWarning, TemperatureAlert>() {
            @Override
            public void flatSelect(Map<String, List<TemperatureWarning>> map, Collector<TemperatureAlert> collector) throws Exception {
                TemperatureWarning begin = map.get("begin").get(0);
                TemperatureWarning next = map.get("next").get(0);
                if (begin.getAverageTemperature() < next.getAverageTemperature()) {
                    collector.collect(new TemperatureAlert(next.getRackID()));
                }
            }
        });

        result.print("告警");

        env.execute();

    }
}
