package common;

import java.io.Serializable;

public class TollNotification implements Serializable {
    public Integer vid;
    public Integer speed;
    public Integer toll;
    public PositionReport pos;
}
