package cn.itcast.function;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.IndexBean;
import cn.itcast.constant.Constant;
import cn.itcast.util.DateUtil;
import cn.itcast.util.FunctionUtil;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MinIndexWindowFunction extends RichWindowFunction<CleanBean, IndexBean,String, TimeWindow> {

    MapState<String, IndexBean> indexMapState;
    @Override
    public void open(Configuration parameters) throws Exception {
        //2.初始化 MapState<String, IndexBean>
        indexMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, IndexBean>("indexMapState", String.class, IndexBean.class));
    }

    @Override
    public void apply(String s, TimeWindow timeWindow, Iterable<CleanBean> input, Collector<IndexBean> collector) throws Exception {
        /**
         * 开发步骤：
         * 1.新建MinIndexWindowFunction 窗口函数
         * 2.初始化 MapState<String, IndexBean>
         * 3.记录最新指数
         * 4.获取分时成交额和成交数量
         * 5.格式化日期
         * 6.封装输出数据
         * 7.更新MapState
         */
        //3.记录最新指数
        CleanBean cleanBean = FunctionUtil.newCleanBean(input);
        IndexBean indexBean = indexMapState.get(cleanBean.getSecCode());
        Long minVol = 0L;
        Long minAmt = 0L;
        if (null != indexBean){
            Long tradeAmtLast = indexBean.getTradeAmtDay();
            Long tradeVolLast = indexBean.getTradeVolDay();
            minVol=cleanBean.getTradeVolumn() - tradeVolLast;
            minAmt=cleanBean.getTradeAmt() - tradeAmtLast;
        }
        Long tradeTime = DateUtil.longTimeTransfer(cleanBean.getEventTime(), Constant.format_YYYYMMDDHHMMSS);

        //6.封装输出数据
        IndexBean newindexBean = new IndexBean(
                cleanBean.getEventTime(),
                cleanBean.getSecCode(),
                cleanBean.getSecName(),
                cleanBean.getPreClosePx(),
                cleanBean.getOpenPrice(),
                cleanBean.getMaxPrice(),
                cleanBean.getMinPrice(),
                cleanBean.getTradePrice(),
                minVol, minAmt,
                cleanBean.getTradeVolumn(),
                cleanBean.getTradeAmt(),
                tradeTime,
                cleanBean.getSource()
        );
        collector.collect(newindexBean);
        indexMapState.put(newindexBean.getIndexCode(), newindexBean);
    }
}
