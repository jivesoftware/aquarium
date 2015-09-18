package com.jivesoftware.os.aquarium;

/**
 *
 * @author jonathan.colt
 */
public class LivelyEndState {

    public static final LivelyEndState ALWAYS_ONLINE = new LivelyEndState(null, Waterline.ALWAYS_ONLINE, Waterline.ALWAYS_ONLINE, null);

    private final CurrentTimeMillis currentTimeMillis;
    public final Waterline currentWaterline;
    public final Waterline desiredWaterline;
    public final Waterline leaderWaterline;

    public LivelyEndState(CurrentTimeMillis currentTimeMillis, Waterline currentWaterline, Waterline desiredWaterline, Waterline leaderWaterline) {
        this.currentTimeMillis = currentTimeMillis;
        this.currentWaterline = currentWaterline;
        this.desiredWaterline = desiredWaterline;
        this.leaderWaterline = leaderWaterline;
    }

    public boolean isOnline() {
        return currentWaterline != null && (currentWaterline.getState() == State.follower || currentWaterline.getState() == State.leader);
    }

    public boolean isLively() {
        if (currentTimeMillis == null) {
            return currentWaterline.isAtQuorum();
        }
        if (currentWaterline != null
            && currentWaterline.isAlive(currentTimeMillis.get())
            && currentWaterline.isAtQuorum()
            && State.checkEquals(currentTimeMillis, currentWaterline, desiredWaterline)) {

            if (desiredWaterline.getState() == State.leader || desiredWaterline.getState() == State.follower) {
                return true;
            }
        }
        return false;
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
            + "currentTimeMillis=" + currentTimeMillis
            + ", currentWaterline=" + currentWaterline
            + ", desiredWaterline=" + desiredWaterline
            + ", leaderWaterline=" + leaderWaterline
            + '}';
    }

}
