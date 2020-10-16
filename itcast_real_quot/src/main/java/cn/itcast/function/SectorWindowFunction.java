package cn.itcast.function;

import cn.itcast.bean.SectorBean;
import cn.itcast.bean.StockBean;
import cn.itcast.util.DbUtil;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SectorWindowFunction extends RichAllWindowFunction<StockBean, SectorBean, TimeWindow> {

    MapState<String, SectorBean> sectorMapSta=null;
    Map<String, List<Map<String, Object>>>  sectorStockMap=null;
    Map<String, Map<String, Object>> sectorDayKlineMap=null;
    BigDecimal basePrice = new BigDecimal(1000);

    @Override
    public void open(Configuration parameters) throws Exception {
        /**
         * 一：open初始化
         *   1. 初始化数据：板块对应关系、最近交易日日K
         *   2. 定义状态MapState<String, SectorBean >:上一个窗口板块数据
         *   3. 初始化基准价
         */
        //上一窗口的的板块数据
        sectorMapSta = getRuntimeContext().getMapState(new MapStateDescriptor<String, SectorBean>("sectorMapSta", String.class, SectorBean.class));

        //板块对应关系
        String sql="SELECT * FROM bdp_sector_stock WHERE sec_abbr = 'ss'";
        //板块下所有个股
        sectorStockMap = DbUtil.queryForGroup("sector_code", sql);

        String sql2 = "SELECT * FROM bdp_quot_sector_kline_day WHERE trade_date = (SELECT last_trade_date FROM tcc_date WHERE trade_date = CURDATE())";
        //板块个股K线
        sectorDayKlineMap  = DbUtil.query("sector_code", sql2);
    }

    @Override
    public void apply(TimeWindow window, Iterable<StockBean> input, Collector<SectorBean> out) throws Exception {
        /**
         *  1.循环窗口内个股数据并缓存
         *  2.轮询板块对应关系表下的个股数据、并获取指定板块下的个股
         *  3.初始化全部数据
         *  4.轮询板块个股数据，并获取板块名称、个股代码、个股流通股本和前一日板块总流通市值
         *  5.计算：开盘价/收盘价累计流通市值、累计（交易量、交易金额）（注意：是个股累计值）
         *     累计个股开盘流通市值 = SUM(板块下的个股开盘价*个股流通股本)
         *     累计个股收盘流通市值 = SUM(板块下的个股收盘价*个股流通股本)
         *  6.判断是否是首日上市，并计算板块开盘价和收盘价
         *     板块开盘价格 = 板块前收盘价*当前板块以开盘价计算的总流通市值/当前板块前一交易日板块总流通市值
         *     板块当前价格 = 板块前收盘价*当前板块以收盘价计算的总流通市值/当前板块前一交易日板块总流通市值
         *  7.初始化板块高低价
         *  8.获取上一个窗口板块数据（高、低、成交量、成交金额）
         *  9.计算最高价和最低价（前后窗口比较）
         *  10.计算分时成交量和成交金额
         *  11.开盘价与高低价比较
         *         12.封装结果数据
         *         13.缓存当前板块数据
         */

        HashMap<String, StockBean> stockCacheMap  = new HashMap<>();
        //1.循环窗口内个股数据并缓存
        for (StockBean stockBean : input) {
            stockCacheMap.put(stockBean.getSecCode(), stockBean);
        }
        //2.轮询板块对应关系表下的个股数据、并获取指定板块下的个股
        for (String sector_code : sectorStockMap.keySet()) {
            List<Map<String, Object>> listStocks  = sectorStockMap.get(sector_code);
         //3.初始化全部数据
            Long eventTime = 0l;
            String sectorName = null;
            BigDecimal preClosePrice = new BigDecimal(0);
            BigDecimal openPrice = new BigDecimal(0);
            BigDecimal highPrice = new BigDecimal(0);
            BigDecimal lowPrice = new BigDecimal(0);
            BigDecimal closePrice = new BigDecimal(0);
            Long tradeVol = 0l; //分时成交量
            Long tradeAmt = 0l; //分时成交额
            Long tradeVolDay = 0l; //日成交总量
            Long tradeAmtDay = 0l; //日成交总额
            Long tradeTime = 0l;
            BigDecimal sectorOpenPrie = new BigDecimal(0);
            BigDecimal sectorClosePrie = new BigDecimal(0);
            //4.轮询板块个股数据，并获取板块名称、个股代码、个股流通股本和前一日板块总流通市值
            for (Map<String, Object> listStock : listStocks) {
                sectorName = listStock.get("sector_name").toString();
                String secCode = listStock.get("sec_code").toString();
                String negoCap = listStock.get("nego_cap").toString();
                String preSectorNegoCapap = listStock.get("pre_sector_nego_cap").toString();
                //5.计算：开盘价/收盘价累计流通市值、累计（交易量、交易金额）（注意：是个股累计值）
                //       累计个股开盘流通市值 = SUM(板块下的个股开盘价*个股流通股本)
                //       累计个股收盘流通市值 = SUM(板块下的个股收盘价*个股流通股本)
                StockBean stockBean = stockCacheMap.get(secCode);
                if (null != stockBean){
                    tradeVolDay = tradeVolDay + stockBean.getTradeVolDay();
                    tradeAmtDay = tradeAmtDay + stockBean.getTradeAmtDay();
                    eventTime=stockBean.getEventTime();
                    tradeTime=stockBean.getTradeTime();
                    sectorOpenPrie = sectorOpenPrie.add(openPrice.multiply(new BigDecimal(negoCap)));
                    sectorClosePrie = sectorClosePrie.add(closePrice.multiply(new BigDecimal(negoCap)));
                }
                /**
                 * 6.判断是否是首日上市，并计算板块开盘价和收盘价
                 *      板块开盘价格 = 板块前收盘价*当前板块以开盘价计算的总流通市值/当前板块前一交易日板块总流通市值
                 *      板块当前价格 = 板块前收盘价*当前板块以收盘价计算的总流通市值/当前板块前一交易日板块总流通市值
                 */
                if (sectorDayKlineMap == null || sectorDayKlineMap.get(sector_code) == null){
                    preClosePrice= basePrice;
                }else {
                    Map<String, Object> map = sectorDayKlineMap.get(sector_code);
                    preClosePrice = new BigDecimal(map.get("close_price").toString());
                }
                //   板块开盘价格 = 板块前收盘价*当前板块以开盘价计算的总流通市值/当前板块前一交易日板块总流通市值
                //   板块当前价格 = 板块前收盘价*当前板块以收盘价计算的总流通市值/当前板块前一交易日板块总流通市值
                openPrice=preClosePrice.multiply(sectorOpenPrie).divide(new BigDecimal(preSectorNegoCapap),2, RoundingMode.HALF_UP).setScale(2, RoundingMode.HALF_UP);
                closePrice=preClosePrice.multiply(sectorClosePrie).divide(new BigDecimal(preSectorNegoCapap),2, RoundingMode.HALF_UP).setScale(2, RoundingMode.HALF_UP);

                SectorBean sectorBeanLast = sectorMapSta.get(sector_code);

                if (null != sectorBeanLast){
                    BigDecimal highPriceLast = sectorBeanLast.getHighPrice();
                    BigDecimal lowPriceLast = sectorBeanLast.getLowPrice();
                    Long tradeAmtDayLast = sectorBeanLast.getTradeAmtDay();
                    Long tradeVolDayLast = sectorBeanLast.getTradeVolDay();
                    //计算最高价,最低价
                    if (highPrice.compareTo(highPriceLast) == -1){
                        highPrice=highPriceLast;
                    }
                    if (lowPrice.compareTo(lowPriceLast) == 1){
                        lowPrice = lowPriceLast;
                    }
                    tradeAmt = tradeAmtDay -sectorBeanLast.getTradeAmtDay();
                    tradeVol = tradeVolDay - sectorBeanLast.getTradeVolDay();
                }
                //开盘价与高低价比较
                if (highPrice.compareTo(openPrice) == -1){
                        highPrice = openPrice;
                }
                if (lowPrice.compareTo(openPrice) == 1){
                    lowPrice =  openPrice;
                }
                //12.封装结果数据
                SectorBean sectorBean = new SectorBean();
                //eventTime、sectorCode、sectorName、preClosePrice、openPrice、highPrice、lowPrice、closePrice、
                //tradeVol、tradeAmt、tradeVolDay、tradeAmtDay、tradeTime
                sectorBean.setEventTime(eventTime);
                sectorBean.setSectorCode(sector_code);
                sectorBean.setSectorName(sectorName);
                sectorBean.setPreClosePrice(preClosePrice);
                sectorBean.setOpenPrice(openPrice);
                sectorBean.setHighPrice(highPrice);
                sectorBean.setLowPrice(lowPrice);
                sectorBean.setClosePrice(closePrice);
                sectorBean.setTradeVol(tradeVol);
                sectorBean.setTradeAmt(tradeAmt);
                sectorBean.setTradeVolDay(tradeVolDay);
                sectorBean.setTradeAmtDay(tradeAmtDay);
                sectorBean.setTradeTime(tradeTime);
                out.collect(sectorBean);

                // 13.缓存当前板块数据
                sectorMapSta.put(sector_code,sectorBean);
            }
        }
    }
}
