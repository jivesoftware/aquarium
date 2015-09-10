package com.jivesoftware.os.aquarium;

/**
 *
 */
public interface StreamQuorumState {

    boolean stream(Waterline waterline) throws Exception;
}
