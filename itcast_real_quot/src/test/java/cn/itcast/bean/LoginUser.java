package cn.itcast.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class LoginUser {
    public int userId; //用户id
    public String ip;//用户Ip
    public String eventType; //状态
    public Long eventTime;//事件时间

}
