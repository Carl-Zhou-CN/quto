package cn.itcast.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class StockIncrBean {
    /**
     * 事件时间
     */
    private Long eventTime;
    /**
     * 证券代码
     */
    private String secCode;
    /**
     * 证券名称
     */
    private String secName;
    /**
     * 涨幅%
     */
    private BigDecimal increase;
    /**
     * 现价
     */
    private BigDecimal tradePrice;
    /**
     * 涨跌
     */
    private BigDecimal updown;
    /**
     * 总手
     */
    private Long tradeVol;
    /**
     * 振幅
     */
    private BigDecimal amplitude;
    /**
     * 前收盘价
     */
    private BigDecimal preClosePrice;
    /**
     * 成交额
     */
    private Long tradeAmt;
    /**
     * 交易时间
     */
    private Long tradeTime;
    /**
     * 数据来源
     */
    private String source;
}
