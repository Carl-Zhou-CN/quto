package cn.itcast.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Product {
    private Long goodsId;
    private Double goodsPrice;
    private String goodsName;
    private String alias;
    private Long orderTime;
    private Boolean status;
}
