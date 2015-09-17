package com.jivesoftware.os.aquarium;

import com.jivesoftware.os.aquarium.ReadWaterlineTx.Tx;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;

/**
 * @author jonathan.colt
 */
public class Aquarium {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final OrderIdProvider versionProvider;
    private final CurrentTimeMillis currentTimeMillis;
    private final ReadWaterlineTx waterlineTx;
    private final TransitionQuorum transitionCurrent;
    private final TransitionQuorum transitionDesired;
    private final Member member;
    private final AwaitLivelyEndState awaitLivelyEndState;

    public Aquarium(OrderIdProvider versionProvider,
        CurrentTimeMillis currentTimeMillis,
        ReadWaterlineTx waterlineTx,
        TransitionQuorum transitionCurrent,
        TransitionQuorum transitionDesired,
        Member member,
        AwaitLivelyEndState awaitLivelyEndState) {
        this.versionProvider = versionProvider;
        this.currentTimeMillis = currentTimeMillis;
        this.waterlineTx = waterlineTx;
        this.transitionCurrent = transitionCurrent;
        this.transitionDesired = transitionDesired;
        this.member = member;
        this.awaitLivelyEndState = awaitLivelyEndState;
    }

    public void inspectState(Tx tx) throws Exception {
        waterlineTx.tx(tx);
    }

    public void tapTheGlass() throws Exception {
        waterlineTx.tx((current, desired) -> {
            current.acknowledgeOther(member);
            desired.acknowledgeOther(member);

            awaitLivelyEndState.notifyChange(() -> {

                while (true) {
                    Waterline currentWaterline = current.get(member);
                    if (currentWaterline == null) {
                        currentWaterline = new Waterline(member, State.bootstrap, versionProvider.nextId(), -1L, true, Long.MAX_VALUE);
                    }
                    Waterline desiredWaterline = desired.get(member);
                    //LOG.info("Tap {} current:{} desired:{}", member, currentWaterline, desiredWaterline);

                    boolean advanced = currentWaterline.getState().transistor.advance(currentTimeMillis,
                        currentWaterline,
                        current,
                        transitionCurrent,
                        desiredWaterline,
                        desired,
                        transitionDesired);
                    if (!advanced) {
                        break;
                    }
                }
                ;
                return captureEndState(member, current, desired) != null;
            });

            return true;
        });
    }

    /**
     * @return null, leader or follower
     */
    public Waterline livelyEndState() throws Exception {
        Waterline[] waterline = { null };
        waterlineTx.tx((current, desired) -> {
            waterline[0] = captureEndState(member, current, desired);
            return true;
        });
        return waterline[0];
    }

    private Waterline captureEndState(Member asMember, ReadWaterline current, ReadWaterline desired) throws Exception {
        Waterline currentWaterline = current.get(asMember);
        Waterline desiredWaterline = desired.get(asMember);

        if (currentWaterline != null
            && currentWaterline.isAlive(currentTimeMillis.get())
            && currentWaterline.isAtQuorum()
            && State.checkEquals(currentTimeMillis, currentWaterline, desiredWaterline)) {

            if (desiredWaterline.getState() == State.leader || desiredWaterline.getState() == State.follower) {
                return desiredWaterline;
            }
        }
        return null;
    }

    public Waterline getLeader() throws Exception {
        Waterline[] leader = { null };
        waterlineTx.tx((current, desired) -> {
            leader[0] = State.highest(member, currentTimeMillis, State.leader, desired, desired.get(member));
            return true;
        });
        return leader[0];
    }

    public Waterline awaitLivelyEndState(long timeoutMillis) throws Exception {
        return awaitLivelyEndState.awaitChange(this::livelyEndState, timeoutMillis);
    }

    public Waterline awaitLeader(long timeoutMillis) throws Exception {
        awaitLivelyEndState.awaitChange(this::livelyEndState, timeoutMillis);
        return getLeader();
    }

    public boolean suggestState(State state) throws Exception {
        return waterlineTx.tx((readCurrent, readDesired) -> transitionDesired.transition(readDesired.get(member), versionProvider.nextId(), state));
    }

    public Waterline getState(Member asMember) throws Exception {
        Waterline[] state = new Waterline[1];
        waterlineTx.tx((readCurrent, readDesired) -> {
            Waterline current = readCurrent.get(asMember);
            if (current == null) {
                state[0] = new Waterline(asMember, State.bootstrap, -1, -1, false, -1);
            } else {
                state[0] = current;
            }
            return true;
        });
        return state[0];
    }

    public void expunge(Member asMember) throws Exception {
        waterlineTx.tx((readCurrent, readDesired) -> {
            transitionDesired.transition(readDesired.get(asMember), versionProvider.nextId(), State.expunged);
            return true;
        });
        tapTheGlass();
    }

    public boolean isLivelyState(Member asMember, State state) throws Exception {
        Waterline waterline = getState(asMember);
        return waterline.getState() == state && waterline.isAtQuorum() && waterline.isAlive(currentTimeMillis.get());
    }

    public boolean isLivelyEndState(Member asMember) throws Exception {
        boolean[] result = { false };
        waterlineTx.tx((readCurrent, readDesired) -> {
            result[0] = captureEndState(asMember, readCurrent, readDesired) != null;
            return true;
        });
        return result[0];
    }
}
