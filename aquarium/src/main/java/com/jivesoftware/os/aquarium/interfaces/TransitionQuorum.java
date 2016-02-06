package com.jivesoftware.os.aquarium.interfaces;

import com.jivesoftware.os.aquarium.ReadWaterline;
import com.jivesoftware.os.aquarium.State;
import com.jivesoftware.os.aquarium.Waterline;
import com.jivesoftware.os.aquarium.WriteWaterline;

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
