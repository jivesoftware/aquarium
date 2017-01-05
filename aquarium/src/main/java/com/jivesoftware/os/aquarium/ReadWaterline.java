package com.jivesoftware.os.aquarium;

import com.google.common.collect.Sets;
import com.jivesoftware.os.aquarium.interfaces.AtQuorum;
import com.jivesoftware.os.aquarium.interfaces.CurrentMembers;
import com.jivesoftware.os.aquarium.interfaces.MemberLifecycle;
import com.jivesoftware.os.aquarium.interfaces.StateStorage;
import com.jivesoftware.os.aquarium.interfaces.StreamQuorumState;
import java.lang.reflect.Array;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;

/**
 * @author jonathan.colt
 */
public class ReadWaterline<T> {

    private final LongAdder getMyWaterline;
    private final LongAdder getOthersWaterline;
    private final LongAdder acknowledgeOther;
    private final StateStorage<T> stateStorage;
    private final MemberLifecycle<T> memberLifecycle;
    private final AtQuorum atQuorum;
    private final CurrentMembers currentMembers;
    private final Class<T> lifecycleType;

    public ReadWaterline(LongAdder getMyWaterline,
        LongAdder getOthersWaterline,
        LongAdder acknowledgeOther,
        StateStorage<T> stateStorage,
        MemberLifecycle<T> memberLifecycle,
        AtQuorum atQuorum,
        CurrentMembers currentMembers,
        Class<T> lifecycleType) {

        this.getMyWaterline = getMyWaterline;
        this.getOthersWaterline = getOthersWaterline;
        this.acknowledgeOther = acknowledgeOther;
        this.stateStorage = stateStorage;
        this.memberLifecycle = memberLifecycle;
        this.atQuorum = atQuorum;
        this.currentMembers = currentMembers;
        this.lifecycleType = lifecycleType;
    }

    public Waterline get(Member asMember) throws Exception {
        getMyWaterline.increment();
        if (!currentMembers.getCurrent().contains(asMember)) {
            return null;
        }

        T lifecycle = memberLifecycle.get(asMember);
        if (lifecycle == null) {
            //LOG.info("Null lifecycle for {}", asMember);
            return null;
        }
        TimestampedState[] current = new TimestampedState[1];
        Set<Member> acked = Sets.newHashSet();
        stateStorage.scan(asMember, null, lifecycle, (rootRingMember, isSelf, ackRingMember, rootLifecycle, state, timestamp, version) -> {
            if (current[0] == null && isSelf) {
                current[0] = new TimestampedState(state, timestamp, version);
            }
            if (current[0] != null) {
                TimestampedState v = new TimestampedState(state, timestamp, version);
                if (v.state == current[0].state && v.timestamp == current[0].timestamp) {
                    acked.add(ackRingMember);
                }
            }
            return true;
        });
        if (current[0] != null) {
            return new Waterline(asMember,
                current[0].state,
                current[0].timestamp,
                current[0].version,
                atQuorum.is(acked.size()));
        } else {
            return null;
        }
    }

    public void getOthers(Member asMember, StreamQuorumState stream) throws Exception {
        getOthersWaterline.increment();

        Member[] otherMember = new Member[1];
        TimestampedState[] otherState = new TimestampedState[1];
        @SuppressWarnings("unchecked")
        T[] otherLifecycle = (T[]) Array.newInstance(lifecycleType, 1);
        Set<Member> acked = Sets.newHashSet();
        Set<Member> current = currentMembers.getCurrent();
        stateStorage.scan(null, null, null, (rootMember, isSelf, ackMember, rootLifecycle, state, timestamp, version) -> {
            if (!current.contains(rootMember) || !isSelf && !current.contains(ackMember)) {
                return true;
            }

            if (otherMember[0] != null && !otherMember[0].equals(rootMember)) {
                boolean otherHasQuorum = atQuorum.is(acked.size());
                stream.stream(new Waterline(otherMember[0],
                    otherState[0].state,
                    otherState[0].timestamp,
                    otherState[0].version,
                    otherHasQuorum));

                otherMember[0] = null;
                otherState[0] = null;
                acked.clear();
            }

            if (otherMember[0] == null && isSelf && !asMember.equals(rootMember)) {
                T lifecycle = memberLifecycle.get(rootMember);
                if (lifecycle != null && rootLifecycle.equals(lifecycle)) {
                    otherMember[0] = rootMember;
                    otherState[0] = new TimestampedState(state, timestamp, version);
                    otherLifecycle[0] = lifecycle;
                }
            }
            if (otherMember[0] != null) {
                TimestampedState v = new TimestampedState(state, timestamp, version);
                if (otherLifecycle[0].equals(rootLifecycle) && v.state == otherState[0].state && v.timestamp == otherState[0].timestamp) {
                    acked.add(ackMember);
                }
            }
            return true;
        });

        if (otherMember[0] != null) {
            boolean otherHasQuorum = atQuorum.is(acked.size());
            stream.stream(new Waterline(otherMember[0],
                otherState[0].state,
                otherState[0].timestamp,
                otherState[0].version,
                otherHasQuorum));
        }
    }

    public void acknowledgeOther(Member member) throws Exception {
        acknowledgeOther.increment();

        stateStorage.update(setState -> {
            @SuppressWarnings("unchecked")
            StateEntry<T>[] otherE = new StateEntry[1];
            boolean[] coldstart = {true};

            //byte[] fromKey = stateKey(versionedPartitionName.getPartitionName(), context, versionedPartitionName.getPartitionVersion(), null, null);
            Set<Member> current = currentMembers.getCurrent();
            stateStorage.scan(null, null, null, (rootMember, isSelf, ackMember, lifecycle, state, timestamp, version) -> {
                if (!current.contains(rootMember) || !isSelf && !current.contains(ackMember)) {
                    return true;
                }

                if (otherE[0] != null && (!otherE[0].rootMember.equals(rootMember) || !otherE[0].lifecycle.equals(lifecycle))) {
                    if (coldstart[0]) {
                        setState.set(otherE[0].rootMember, member, otherE[0].lifecycle, otherE[0].state, otherE[0].timestamp);
                    }
                    otherE[0] = null;
                    coldstart[0] = true;
                }

                if (otherE[0] == null && isSelf && !member.equals(rootMember)) {
                    otherE[0] = new StateEntry<>(rootMember, lifecycle, state, timestamp);
                }
                if (otherE[0] != null && member.equals(ackMember)) {
                    coldstart[0] = false;
                    if (state != otherE[0].state || timestamp != otherE[0].timestamp) {
                        setState.set(otherE[0].rootMember, member, otherE[0].lifecycle, otherE[0].state, otherE[0].timestamp);
                    }
                }
                return true;
            });
            if (otherE[0] != null && coldstart[0]) {
                setState.set(otherE[0].rootMember, member, otherE[0].lifecycle, otherE[0].state, otherE[0].timestamp);
            }
            return true;
        });
    }

    private static class StateEntry<T> {

        private final Member rootMember;
        private final T lifecycle;
        private final State state;
        private final long timestamp;

        public StateEntry(Member rootMember,
            T lifecycle,
            State state,
            long timestamp) {
            this.rootMember = rootMember;
            this.lifecycle = lifecycle;
            this.state = state;
            this.timestamp = timestamp;
        }
    }

    private static class TimestampedState {

        private final State state;
        private final long timestamp;
        private final long version;

        public TimestampedState(State state, long timestamp, long version) {
            this.state = state;
            this.timestamp = timestamp;
            this.version = version;
        }

        @Override
        public String toString() {
            return "TimestampedState{"
                + "state=" + state
                + ", timestamp=" + timestamp
                + ", version=" + version
                + '}';
        }
    }

}
