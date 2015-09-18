package com.jivesoftware.os.aquarium;

/**
 *
 * @author jonathan.colt
 */
public interface ReadWaterlineTx {

    <R> R tx(Tx<R> tx) throws Exception;

    interface Tx<R> {

        R tx(ReadWaterline readCurrent, ReadWaterline readDesired) throws Exception;
    }

}
