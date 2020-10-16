package cn.itcast.task.source;

/**
 * 首先我们定义一个监控事件实体类
 */
public abstract class MonitoringEvent {
    private int rackID;

    public MonitoringEvent(int rackID) {
        this.rackID = rackID;
    }

    public int getRackID() {
        return rackID;
    }

    public void setRackID(int rackID) {
        this.rackID = rackID;
    }
}