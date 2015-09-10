package com.jivesoftware.os.aquarium;

import java.util.concurrent.Callable;

/**
 *
 */
public interface AwaitLivelyEndState {

    Waterline awaitChange(Callable<Waterline> awaiter, long timeoutMillis) throws Exception;

    void notifyChange(Callable<Boolean> change) throws Exception;
}