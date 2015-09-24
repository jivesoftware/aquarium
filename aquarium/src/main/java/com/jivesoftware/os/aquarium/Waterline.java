package com.jivesoftware.os.aquarium;

/**
 *
 */
public class Waterline {

    public static final Waterline ALWAYS_ONLINE = new Waterline(null,  State.follower, 0, 0, true);

    private final Member member;
    private final State state;
    private final long timestamp;
    private final long version;
    private final boolean atQuorum;

    public Waterline(Member member, State state, long timestamp, long version, boolean atQuorum) {
        this.member = member;
        this.state = state;
        this.timestamp = timestamp;
        this.version = version;
        this.atQuorum = atQuorum;
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
            '}';
    }

}
