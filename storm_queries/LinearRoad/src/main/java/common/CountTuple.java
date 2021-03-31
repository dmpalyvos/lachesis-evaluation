package common;

import java.io.Serializable;

public class CountTuple implements Serializable {
    public Short minuteNumber;
    public Integer xway;
    public Short segment;
    public Short direction;
    public Integer count;

    public boolean isProgressTuple() {
        return xway == null;
    }
}
