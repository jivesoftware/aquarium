package com.jivesoftware.os.aquarium;

import java.util.EnumMap;
import java.util.concurrent.atomic.LongAdder;

/**
 *
 * @author jonathan.colt
 */
public class AquariumStats {

    public final LongAdder feedTheFish = new LongAdder();
    public final LongAdder acknowledgeOther = new LongAdder();
    public final LongAdder tapTheGlass = new LongAdder();
    public final LongAdder tapTheGlassNotified = new LongAdder();
    public final LongAdder captureEndState = new LongAdder();

    public final LongAdder getLivelyEndState = new LongAdder();
    public final LongAdder suggestState = new LongAdder();
    public final LongAdder getLeader = new LongAdder();

    public final LongAdder getStateForMember = new LongAdder();
    public final LongAdder isLivelyStateForMember = new LongAdder();
    public final LongAdder isLivelyEndStateForMember = new LongAdder();

    public final LongAdder awaitOnline = new LongAdder();
    public final LongAdder awaitTimedOut = new LongAdder();

    public final LongAdder getMyDesiredWaterline = new LongAdder();
    public final LongAdder getOthersDesiredWaterline = new LongAdder();
    public final LongAdder acknowledgeDesiredOther = new LongAdder();

    public final LongAdder getMyCurrentWaterline = new LongAdder();
    public final LongAdder getOthersCurrentWaterline = new LongAdder();
    public final LongAdder acknowledgeCurrentOther = new LongAdder();

    public final EnumMap<State, LongAdder> desiredState = new EnumMap<>(State.class);
    public final EnumMap<State, LongAdder> currentState = new EnumMap<>(State.class);

    public AquariumStats() {
        desiredState.put(State.bootstrap, new LongAdder());
        desiredState.put(State.inactive, new LongAdder());
        desiredState.put(State.nominated, new LongAdder());
        desiredState.put(State.leader, new LongAdder());
        desiredState.put(State.follower, new LongAdder());
        desiredState.put(State.demoted, new LongAdder());
        desiredState.put(State.expunged, new LongAdder());

        currentState.put(State.bootstrap, new LongAdder());
        currentState.put(State.inactive, new LongAdder());
        currentState.put(State.nominated, new LongAdder());
        currentState.put(State.leader, new LongAdder());
        currentState.put(State.follower, new LongAdder());
        currentState.put(State.demoted, new LongAdder());
        currentState.put(State.expunged, new LongAdder());
    }

}
