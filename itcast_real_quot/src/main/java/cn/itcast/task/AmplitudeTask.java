package cn.itcast.task;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.WarnAmplitudeBean;
import cn.itcast.bean.WarnBaseBean;
import cn.itcast.inter.ProcessDataCepInterface;
import cn.itcast.mail.MailSend;
import cn.itcast.util.RedisUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import redis.clients.jedis.JedisCluster;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;


public class AmplitudeTask implements ProcessDataCepInterface {
    @Override
    public void process(DataStream<CleanBean> waterData, StreamExecutionEnvironment env) {
        SingleOutputStreamOperator<WarnBaseBean> mapData = waterData.map(new MapFunction<CleanBean, WarnBaseBean>() {
            @Override
            public WarnBaseBean map(CleanBean cleanBean) throws Exception {
                return new WarnBaseBean(
                        cleanBean.getSecCode(),
                        cleanBean.getPreClosePx(),
                        cleanBean.getMaxPrice(),
                        cleanBean.getMinPrice(),
                        cleanBean.getTradePrice(),
                        cleanBean.getEventTime()
                );
            }
        });

        StreamTableEnvironment tblEnv = TableEnvironment.getTableEnvironment(env);

        tblEnv.registerDataStream("tbl", waterData, "secCode, lowPrice, highPrice, preClosePrice, eventTime.rowtime");
        //振幅 = (max - min) / preClosePx
        String sql = "select secCode,preClosePrice,max(maxPrice),min(minPrice) from tbl group by secCode,preClosePrice,tumble(eventTime,interval '2' second)";

        Table table = tblEnv.sqlQuery(sql);

        DataStream<WarnAmplitudeBean> WarnAmplitudeData = tblEnv.toAppendStream(table, TypeInformation.of(WarnAmplitudeBean.class));

        JedisCluster jedisCluster = RedisUtil.getJedisCluster();
        String amplThreshold = jedisCluster.hget("quot", "amplitude");

        Pattern<WarnAmplitudeBean, WarnAmplitudeBean> pattern = Pattern.<WarnAmplitudeBean>begin("begin")
                .where(new SimpleCondition<WarnAmplitudeBean>() {
                    @Override
                    public boolean filter(WarnAmplitudeBean warnAmplitudeBean) throws Exception {
                        BigDecimal amplVal = (warnAmplitudeBean.getHighPrice().multiply(warnAmplitudeBean.getLowPrice())).divide(warnAmplitudeBean.getPreClosePrice(), 2, RoundingMode.HALF_UP);
                        if (amplVal.compareTo(new BigDecimal(amplThreshold)) == 1) {
                            return true;
                        }
                        return false;
                    }
                });

        PatternStream<WarnAmplitudeBean> cep = CEP.pattern(WarnAmplitudeData.keyBy(WarnAmplitudeBean ::getSecCode), pattern);

        cep.select(new PatternSelectFunction<WarnAmplitudeBean, Object>() {
            @Override
            public Object select(Map<String, List<WarnAmplitudeBean>> map) throws Exception {
                List<WarnAmplitudeBean> begin = map.get("begin");
                if (!begin.isEmpty()){
                    MailSend.send("振幅警告数据" + begin.toString());
                }
                return begin;
            }
        }).print();

    }
}
