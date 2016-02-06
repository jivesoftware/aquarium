package com.jivesoftware.os.aquarium;

import com.jivesoftware.os.aquarium.interfaces.IsMemberAlive;

/**
 * @author jonathan.colt
 */
public class LivelyEndState {

    public static final LivelyEndState ALWAYS_ONLINE = new LivelyEndState(null, Waterline.ALWAYS_ONLINE, Waterline.ALWAYS_ONLINE, null);

    private final IsMemberAlive isMemberAlive;
    private final Waterline currentWaterline;
    private final Waterline desiredWaterline;
    private final Waterline leaderWaterline;

    public LivelyEndState(IsMemberAlive isMemberAlive, Waterline currentWaterline, Waterline desiredWaterline, Waterline leaderWaterline) {
        this.isMemberAlive = isMemberAlive;
        this.currentWaterline = currentWaterline;
        this.desiredWaterline = desiredWaterline;
        this.leaderWaterline = leaderWaterline;
    }

    public State getCurrentState() {
        return currentWaterline != null ? currentWaterline.getState() : null;
    }

    public Waterline getCurrentWaterline() {
        return currentWaterline;
    }

    public Waterline getLeaderWaterline() {
        return leaderWaterline;
    }

    public boolean isOnline() throws Exception {
        if (isMemberAlive == null) {
            return currentWaterline.isAtQuorum();
        }
        return currentWaterline != null
            && currentWaterline.isAtQuorum()
            && (currentWaterline.getState() == State.follower || currentWaterline.getState() == State.leader)
            && isMemberAlive.isAlive(currentWaterline.getMember())
            && Waterline.checkEquals(currentWaterline, desiredWaterline);
    }

    @Override
    public boolean equals(Object obj) {
        throw new UnsupportedOperationException("Stop that");
    }

    @Override
    public int hashCode() {
        throw new UnsupportedOperationException("Stop that");
    }

    @Override
    public String toString() {
        return "LivelyEndState{"
            + "isMemberAlive=" + isMemberAlive
            + ", currentWaterline=" + currentWaterline
            + ", desiredWaterline=" + desiredWaterline
            + ", leaderWaterline=" + leaderWaterline
            + '}';
    }

}
