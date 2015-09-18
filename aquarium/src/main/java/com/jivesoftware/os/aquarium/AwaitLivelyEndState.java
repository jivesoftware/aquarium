package com.jivesoftware.os.aquarium;

import java.util.concurrent.Callable;

/**
 *
 */
public interface AwaitLivelyEndState {

    LivelyEndState awaitChange(Callable<LivelyEndState> awaiter, long timeoutMillis) throws Exception;

    void notifyChange(Callable<Boolean> change) throws Exception;
}