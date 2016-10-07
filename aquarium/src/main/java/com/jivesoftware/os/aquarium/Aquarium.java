package com.jivesoftware.os.aquarium;

import com.jivesoftware.os.aquarium.interfaces.AtQuorum;
import com.jivesoftware.os.aquarium.interfaces.AwaitLivelyEndState;
import com.jivesoftware.os.aquarium.interfaces.IsCurrentMember;
import com.jivesoftware.os.aquarium.interfaces.MemberLifecycle;
import com.jivesoftware.os.aquarium.interfaces.StateStorage;
import com.jivesoftware.os.aquarium.interfaces.TransitionQuorum;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;

/**
 * @author jonathan.colt
 */
public class Aquarium {

    private final AquariumStats aquariumStats;
    private final OrderIdProvider versionProvider;
    private final TransitionQuorum transitionCurrent;
    private final TransitionQuorum transitionDesired;
    private final Liveliness liveliness;
    private final Member member;
    private final AwaitLivelyEndState awaitLivelyEndState;

    private final ReadWaterline readCurrent;
    private final ReadWaterline readDesired;

    private final WriteWaterline writeCurrent;
    private final WriteWaterline writeDesired;

    public <T> Aquarium(AquariumStats aquariumStats,
        OrderIdProvider versionProvider,
        StateStorage<T> currentStateStorage,
        StateStorage<T> desiredStateStorage,
        TransitionQuorum current,
        TransitionQuorum desired,
        Liveliness liveliness,
        MemberLifecycle<T> memberLifecycle,
        Class<T> lifecycleClass,
        AtQuorum atQuorum,
        IsCurrentMember isCurrentMember,
        Member member,
        AwaitLivelyEndState awaitLivelyEndState) {

        this.aquariumStats = aquariumStats;
        this.versionProvider = versionProvider;
        this.transitionCurrent = (existing, nextTimestamp, nextState, readCurrent, readDesired, writeCurrent, writeDesired) -> {
            boolean transitioned = current.transition(existing, nextTimestamp, nextState, readCurrent, readDesired, writeCurrent, writeDesired);
            if (transitioned) {
                if (existing != null && existing.getState() != null) {
                    aquariumStats.currentState.get(existing.getState()).decrement();
                } else {
                    aquariumStats.desiredState.get(State.bootstrap).decrement();
                    aquariumStats.currentState.get(State.bootstrap).decrement();
                }
                aquariumStats.currentState.get(nextState).increment();
            }
            return transitioned;
        };
        this.transitionDesired = (existing, nextTimestamp, nextState, readCurrent, readDesired, writeCurrent, writeDesired) -> {
            boolean transitioned = desired.transition(existing, nextTimestamp, nextState, readCurrent, readDesired, writeCurrent, writeDesired);
            if (transitioned) {
                if (existing != null && existing.getState() != null) {
                    aquariumStats.desiredState.get(existing.getState()).decrement();
                } else {
                    aquariumStats.desiredState.get(State.bootstrap).decrement();
                    aquariumStats.currentState.get(State.bootstrap).decrement();
                }
                aquariumStats.desiredState.get(nextState).increment();
            }
            return transitioned;
        };
        this.liveliness = liveliness;
        this.member = member;
        this.awaitLivelyEndState = awaitLivelyEndState;

        this.readCurrent = new ReadWaterline<>(
            aquariumStats.getMyCurrentWaterline,
            aquariumStats.getOthersCurrentWaterline,
            aquariumStats.acknowledgeCurrentOther,
            currentStateStorage,
            memberLifecycle,
            atQuorum,
            isCurrentMember,
            lifecycleClass);

        this.readDesired = new ReadWaterline<>(
            aquariumStats.getMyDesiredWaterline,
            aquariumStats.getOthersDesiredWaterline,
            aquariumStats.acknowledgeDesiredOther,
            desiredStateStorage,
            memberLifecycle,
            atQuorum,
            isCurrentMember,
            lifecycleClass);

        this.writeCurrent = new WriteWaterline<>(currentStateStorage, memberLifecycle);
        this.writeDesired = new WriteWaterline<>(desiredStateStorage, memberLifecycle);
    }

    public interface Tx<R> {

        R tx(ReadWaterline readCurrent, ReadWaterline readDesired, WriteWaterline writeCurrent, WriteWaterline writeDesired) throws Exception;
    }

    public <R> R tx(Tx<R> tx) throws Exception {
        return tx.tx(readCurrent, readDesired, writeCurrent, writeDesired);
    }

    public void acknowledgeOther() throws Exception {
        aquariumStats.acknowledgeOther.increment();
        readCurrent.acknowledgeOther(member);
        readDesired.acknowledgeOther(member);
    }

    public void tapTheGlass() throws Exception {
        aquariumStats.tapTheGlass.increment();
        awaitLivelyEndState.notifyChange(() -> {

            while (true) {
                Waterline currentWaterline = readCurrent.get(member);
                if (currentWaterline == null) {
                    currentWaterline = new Waterline(member, State.bootstrap, versionProvider.nextId(), -1L, true);
                    aquariumStats.currentState.get(State.bootstrap).increment();
                }
                Waterline desiredWaterline = readDesired.get(member);
                //LOG.info("Tap {} current:{} desired:{}", member, currentWaterline, desiredWaterline);

                boolean advanced = currentWaterline.getState().transistor.advance(liveliness,
                    currentWaterline,
                    readCurrent,
                    writeCurrent,
                    transitionCurrent,
                    desiredWaterline,
                    readDesired,
                    writeDesired,
                    transitionDesired);
                if (!advanced) {
                    break;
                }
            }

            return captureEndState(member, readCurrent, readDesired) != null;
        });
    }

    /**
     * @return null, leader or follower
     */
    public LivelyEndState livelyEndState() throws Exception {
        aquariumStats.getLivelyEndState.increment();
        Waterline current = readCurrent.get(member);
        Waterline desired = readDesired.get(member);
        return new LivelyEndState(liveliness,
            current,
            desired,
            State.highest(member, State.leader, readDesired, desired));
    }

    private Waterline captureEndState(Member asMember, ReadWaterline current, ReadWaterline desired) throws Exception {
        Waterline currentWaterline = current.get(asMember);
        Waterline desiredWaterline = desired.get(asMember);

        if (currentWaterline != null
            && liveliness.isAlive(asMember)
            && currentWaterline.isAtQuorum()
            && Waterline.checkEquals(currentWaterline, desiredWaterline)) {

            if (desiredWaterline.getState() == State.leader || desiredWaterline.getState() == State.follower) {
                return desiredWaterline;
            }
        }
        return null;
    }

    public Waterline getLeader() throws Exception {
        aquariumStats.getLeader.increment();
        return State.highest(member, State.leader, readDesired, readDesired.get(member));
    }

    public LivelyEndState awaitOnline(long timeoutMillis) throws Exception {
        try {
            aquariumStats.awaitOnline.increment();
            LivelyEndState endState = awaitLivelyEndState.awaitChange(() -> {
                LivelyEndState livelyEndState = livelyEndState();
                return livelyEndState.isOnline() ? livelyEndState : null;
            }, timeoutMillis);

            return endState;
        } catch (Exception x) {
            aquariumStats.awaitTimedOut.increment();
            throw x;
        }
    }

    public boolean suggestState(State state) throws Exception {

        return transitionDesired.transition(readDesired.get(member),
            versionProvider.nextId(),
            state,
            readCurrent,
            readDesired,
            writeCurrent,
            writeDesired);
    }

    public Waterline getState(Member asMember) throws Exception {
        Waterline current = readCurrent.get(asMember);
        return (current != null) ? current : new Waterline(asMember, State.bootstrap, -1, -1, false);
    }

    public boolean isLivelyState(Member asMember, State state) throws Exception {
        if (!liveliness.isAlive(asMember)) {
            return false;
        }
        Waterline waterline = getState(asMember);
        return waterline.getState() == state && waterline.isAtQuorum();
    }

    public boolean isLivelyEndState(Member asMember) throws Exception {
        return captureEndState(asMember, readCurrent, readDesired) != null;
    }
}
