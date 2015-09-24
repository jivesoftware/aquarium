package com.jivesoftware.os.aquarium;

import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;

/**
 * @author jonathan.colt
 */
public class WriteWaterline<T> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final StateStorage<T> stateStorage;
    private final MemberLifecycle<T> memberLifecycle;

    public WriteWaterline(StateStorage<T> stateStorage,
        MemberLifecycle<T> memberLifecycle) {
        this.stateStorage = stateStorage;
        this.memberLifecycle = memberLifecycle;
    }

    public boolean put(Member asMember, State state, long timestamp) throws Exception {
        return stateStorage.update(setState -> setState.set(asMember, asMember, memberLifecycle.get(asMember), state, timestamp));
    }

}
