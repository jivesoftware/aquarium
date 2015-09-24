package com.jivesoftware.os.aquarium;

/**
 *
 * @author jonathan.colt
 */
public interface TransitionQuorum {

    boolean transition(Waterline existing,
        long nextTimestamp,
        State nextState,
        ReadWaterline readCurrent,
        ReadWaterline readDesired,
        WriteWaterline writeCurrent,
        WriteWaterline writeDesired) throws Exception;
}
