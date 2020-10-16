package cn.itcast.function;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.StockBean;
import cn.itcast.constant.Constant;
import cn.itcast.util.DateUtil;
import cn.itcast.util.FunctionUtil;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


public class MinStockWindowFunction extends RichWindowFunction<CleanBean, StockBean, String, TimeWindow> {

    /**
     * 1.新建MinStockWindowFunction 窗口函数
     * 2.初始化 MapState<String, StockBean>
     * 3.记录最新个股
     * 4.获取分时成交额和成交数量
     * 5.格式化日期
     * 6.封装输出数据
     * 7.更新MapState
     */
    private MapState<String, StockBean> mapState;

    //2.初始化 MapState<String, StockBean>
    @Override
    public void open(Configuration parameters) throws Exception {
        mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, StockBean>("stockState", String.class, StockBean.class));
    }

    @Override
    public void apply(String s, TimeWindow timeWindow, Iterable<CleanBean> input, Collector<StockBean> collector) throws Exception {
        //3.记录最新个股
        CleanBean cleanBean = FunctionUtil.newCleanBean(input);
        //4.获取分时成交额和成交数量
        StockBean stockBeanMap = mapState.get(cleanBean.getSecCode());
        long minTradeVol = 0L;
        long minTradeAmt = 0L;
        if (null != stockBeanMap) {
            //根据窗口计算每个窗口的成交量和成交数量
            Long tradeVolLast = stockBeanMap.getTradeVolDay();
            Long tradeAmtLast = stockBeanMap.getTradeAmtDay();
            minTradeVol = cleanBean.getTradeVolumn() - tradeVolLast;
            minTradeAmt = cleanBean.getTradeAmt() - tradeAmtLast;
        }
        Long tradeTime = DateUtil.longTimeTransfer(cleanBean.getEventTime(), Constant.format_YYYYMMDDHHMMSS);
        StockBean stockBean = new StockBean(
                cleanBean.getEventTime(),
                cleanBean.getSecCode(),
                cleanBean.getSecName(),
                cleanBean.getPreClosePx(),
                cleanBean.getOpenPrice(),
                cleanBean.getMaxPrice(),
                cleanBean.getMinPrice(),
                cleanBean.getTradePrice(),
                minTradeVol, minTradeAmt,
                cleanBean.getTradeVolumn(),
                cleanBean.getTradeAmt(),
                tradeTime,
                cleanBean.getSource()
        );
        collector.collect(stockBean);
        mapState.put(stockBean.getSecCode(),stockBean);
    }
}
