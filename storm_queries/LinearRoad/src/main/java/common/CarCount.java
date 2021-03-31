package common;

import java.io.Serializable;

/**
 * {@link CarCount} is an class that helps to count the number of cars in a segment. Its initial count value is one.
 *
 * @author mjsax
 */
public final class CarCount implements Serializable {
    /**
     * The current count.
     */
    public int count = 1;
}
