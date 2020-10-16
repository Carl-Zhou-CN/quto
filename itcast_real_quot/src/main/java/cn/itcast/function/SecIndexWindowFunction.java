package cn.itcast.function;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.IndexBean;
import cn.itcast.constant.Constant;
import cn.itcast.util.DateUtil;
import cn.itcast.util.FunctionUtil;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class SecIndexWindowFunction implements WindowFunction<CleanBean,IndexBean,String, TimeWindow> {
    @Override
    public void apply(String s, TimeWindow timeWindow, Iterable<CleanBean> input, Collector<IndexBean> collector) throws Exception {
        /**
         * 开发步骤：
         * 1.新建SecIndexWindowFunction 窗口函数
         * 2.记录最新指数
         * 3.格式化日期
         * 4.封装输出数据
         */
        //2.记录最新指数
        CleanBean cleanBean = FunctionUtil.newCleanBean(input);
        //3.格式化日期
        Long tradeTime = DateUtil.longTimeTransfer(cleanBean.getEventTime(), Constant.format_YYYYMMDDHHMMSS);
        //4.封装输出数据
        IndexBean indexBean = new IndexBean(
                cleanBean.getEventTime(),
                cleanBean.getSecCode(),
                cleanBean.getSecName(),
                cleanBean.getPreClosePx(),
                cleanBean.getOpenPrice(),
                cleanBean.getMaxPrice(),
                cleanBean.getMinPrice(),
                cleanBean.getTradePrice(),
                0L, 0L,
                cleanBean.getTradeVolumn(),
                cleanBean.getTradeAmt(),
                tradeTime,
                cleanBean.getSource()
        );
        collector.collect(indexBean);
    }
}
