package cn.itcast.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class LoginEvent {
    private String id; //用户id
    private String ip;//用户ip
    private String status;//用户状态
    private int count;//失败次数
}
