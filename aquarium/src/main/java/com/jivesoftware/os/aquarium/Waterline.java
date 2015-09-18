package com.jivesoftware.os.aquarium;

/**
 *
 */
public class Waterline {

    public static final Waterline ALWAYS_ONLINE = new Waterline(null,  State.follower, 0, 0, true, Long.MAX_VALUE);

    private final Member member;
    private final State state;
    private final long timestamp;
    private final long version;
    private final boolean atQuorum;
    private final long aliveUntilTimestamp;

    public Waterline(Member member, State state, long timestamp, long version, boolean atQuorum, long aliveUntilTimestamp) {
        this.member = member;
        this.state = state;
        this.timestamp = timestamp;
        this.version = version;
        this.atQuorum = atQuorum;
        this.aliveUntilTimestamp = aliveUntilTimestamp;
    }

    public Member getMember() {
        return member;
    }

    public State getState() {
        return state;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getVersion() {
        return version;
    }

    public boolean isAtQuorum() {
        return atQuorum;
    }

    public boolean isAlive(long currentTimeMillis) {
        return currentTimeMillis <= aliveUntilTimestamp;
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
        return "Waterline{" +
            "member=" + member +
            ", state=" + state +
            ", timestamp=" + timestamp +
            ", version=" + version +
            ", atQuorum=" + atQuorum +
            ", aliveUntilTimestamp=" + aliveUntilTimestamp +
            '}';
    }

}
