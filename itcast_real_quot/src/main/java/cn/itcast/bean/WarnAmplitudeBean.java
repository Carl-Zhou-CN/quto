package cn.itcast.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class WarnAmplitudeBean {
    /**
     * 证券代码
     */
    private String secCode;
    /**
     * 昨收盘价
     */
    private BigDecimal preClosePrice;
    /**
     * 当日最高价
     */
    private BigDecimal highPrice;
    /**
     * 当日最低价
     */
    private BigDecimal lowPrice;
}
