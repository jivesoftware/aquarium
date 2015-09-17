package com.jivesoftware.os.aquarium;

/**
 *
 * @author jonathan.colt
 */
public interface ReadWaterlineTx {

    boolean tx(Tx tx) throws Exception;

    interface Tx {

        boolean tx(ReadWaterline readCurrent, ReadWaterline readDesired) throws Exception;
    }

}
