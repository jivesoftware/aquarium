package com.jivesoftware.os.aquarium.interfaces;

import com.jivesoftware.os.aquarium.Waterline;

/**
 *
 */
public interface StreamQuorumState {

    boolean stream(Waterline waterline) throws Exception;
}
