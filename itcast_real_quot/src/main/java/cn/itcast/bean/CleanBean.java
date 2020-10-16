package cn.itcast.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
@NoArgsConstructor
@AllArgsConstructor
@Data
public class CleanBean {
    /** 行情类型 */
    private String mdStreamId;
    /** 产品代码 */
    private String secCode;
    /** 产品名称*/
    private String secName;
    /** 成交量*/
    private Long tradeVolumn;
    /** 成交额*/
    private Long tradeAmt;
    /** 昨收盘价*/
    private BigDecimal preClosePx;
    /** 开盘价*/
    private BigDecimal openPrice;
    /** 当日最高价*/
    private BigDecimal maxPrice;
    /** 当日最低价*/
    private BigDecimal minPrice;
    /** 当前价*/
    private BigDecimal tradePrice;
    /** 数据事件时间,窗口触发依据*/
    private Long eventTime;
    /** 数据来源*/
    private String source;
}
