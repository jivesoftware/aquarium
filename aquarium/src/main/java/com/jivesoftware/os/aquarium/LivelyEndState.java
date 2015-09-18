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
        if (currentTimeMillis == null) {
            return currentWaterline.isAtQuorum();
        }
        return currentWaterline != null
            && currentWaterline.isAtQuorum()
            && (currentWaterline.getState() == State.follower || currentWaterline.getState() == State.leader)
            && currentWaterline.isAlive(currentTimeMillis.get())
            && State.checkEquals(currentTimeMillis.get(), currentWaterline, desiredWaterline);
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
