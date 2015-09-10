package com.jivesoftware.os.aquarium;

/**
 *
 * @author jonathan.colt
 */
public interface TransitionQuorum {

    boolean transition(Waterline current, long desiredTimestamp, State state) throws Exception;
}
