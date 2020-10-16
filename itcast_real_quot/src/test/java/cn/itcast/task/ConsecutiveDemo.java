package cn.itcast.task;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Map;

public class ConsecutiveDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 模拟数据源
        //输入事件：c d a a a d a b
        DataStreamSource<String> source = env.fromElements("c", "d", "a", "a", "a", "d", "a", "b");

        Pattern<String, String> pattern = Pattern.<String>begin("start")
                .where(new SimpleCondition<String>() {
                    @Override
                    public boolean filter(String s) throws Exception {
                        return s.equals("c");
                    }
                })
                .followedBy("middle")
                .where(new SimpleCondition<String>() {
                    @Override
                    public boolean filter(String s) throws Exception {
                        return s.equals("a");
                    }
                })
                .oneOrMore()
               // .consecutive()//连续匹配
                .allowCombinations()//允许组合
                .followedBy("end")
                .where(new SimpleCondition<String>() {
                    @Override
                    public boolean filter(String s) throws Exception {
                        return s.equals("b");
                    }
                });
        CEP.<String>pattern(source, pattern)
                .select(new PatternSelectFunction<String, Object>() {
                    @Override
                    public Object select(Map<String, List<String>> map) throws Exception {
                        List<String> middle = map.get("middle");
                        List<String> start = map.get("start");
                        List<String> end = map.get("end");
                        Tuple3 tuple3 = new Tuple3(start, middle, end);
                        return tuple3;
                    }
                }).print();

        env.execute();
    }
}
