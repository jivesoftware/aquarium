package com.jivesoftware.os.aquarium;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.aquarium.interfaces.AtQuorum;
import com.jivesoftware.os.aquarium.interfaces.AwaitLivelyEndState;
import com.jivesoftware.os.aquarium.interfaces.CurrentTimeMillis;
import com.jivesoftware.os.aquarium.interfaces.CurrentMembers;
import com.jivesoftware.os.aquarium.interfaces.LivelinessStorage;
import com.jivesoftware.os.aquarium.interfaces.MemberLifecycle;
import com.jivesoftware.os.aquarium.interfaces.StateStorage;
import com.jivesoftware.os.aquarium.interfaces.TransitionQuorum;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import java.util.Arrays;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author jonathan.colt
 */
public class AquariumNGTest {

    private static final Member MIN = new Member(intBytes(0));
    private static final Member MAX = new Member(intBytes(Integer.MAX_VALUE));
    private static final byte CURRENT = 0;
    private static final byte DESIRED = 1;
    private static final byte LIVELINESS = 2;

    @Test
    public void testTapTheGlassSingleNode() throws Exception {

        NavigableMap<Key, TimestampedState<State>> rawState = new ConcurrentSkipListMap<>();
        NavigableMap<Key, TimestampedState<Void>> rawLiveliness = new ConcurrentSkipListMap<>();

        AtomicInteger rawRingSize = new AtomicInteger();
        AtQuorum atQuorum = count -> count > rawRingSize.get() / 2;


        Map<Member, Integer> rawLifecycles = Maps.newConcurrentMap();
        CurrentMembers currentMembers = rawLifecycles::keySet;

        int aquariumNodeCount = 1;
        AquariumNode[] nodes = new AquariumNode[aquariumNodeCount];
        int deadAfterMillis = 10_000;
        for (int i = 0; i < aquariumNodeCount; i++) {
            createNode(i, rawLifecycles, rawLiveliness, rawState, atQuorum, currentMembers, deadAfterMillis, nodes);
        }

        ScheduledExecutorService service = Executors.newScheduledThreadPool(aquariumNodeCount);

        int running = 0;
        AquariumNode[] alive = new AquariumNode[aquariumNodeCount];
        Future[] aliveFutures = new Future[aquariumNodeCount];
        for (; running < 1; running++) {
            rawLifecycles.compute(nodes[running].member, (member, value) -> (value != null) ? (value + 1) : 0);
            aliveFutures[running] = service.scheduleWithFixedDelay(nodes[running], 10, 10, TimeUnit.MILLISECONDS);
            rawRingSize.incrementAndGet();
            alive[running] = nodes[running];
        }
        String mode = "Start with " + rawRingSize + " nodes...";
        awaitLeader(mode, alive, rawRingSize);

        mode = "Force each node to be a leader...";
        AquariumNode leader = null;
        for (int i = 0; i < running; i++) {
            nodes[i].forceDesiredState(State.leader);
            leader = awaitLeader(mode, alive, rawRingSize);
        }

        mode = "Force each leader to follower and the back to leader...";
        for (int i = 0; i < running; i++) {

            leader.forceDesiredState(State.follower);
            leader.forceDesiredState(State.leader);
            leader = awaitLeader(mode, alive, rawRingSize);
        }

        mode = "Force all nodes to be a leader...";
        for (int i = 0; i < running; i++) {
            nodes[i].forceDesiredState(State.leader);
        }
        awaitLeader(mode, alive, rawRingSize);

        mode = "Force each node to be a follower...";
        for (int i = 0; i < running; i++) {
            if (i < running - 1) {
                nodes[i].forceDesiredState(State.follower);
            }
            leader = awaitLeader(mode, alive, rawRingSize);
        }

        mode = "Prove immune to clock drift by moving leader...";
        for (int i = 0; i < 2; i++) {
            leader.clockDrift.set(20_000 * ((i % 2 == 0) ? -1 : 1));
            AquariumNode newLeader = awaitLeader(mode, alive, rawRingSize);
            leader.clockDrift.set(0);
            leader = newLeader;
        }

        mode = "Prove immune to clock drift by moving followers...";
        for (int i = 0; i < running; i++) {
            for (int j = 0; j < running; j++) {
                if (nodes[j] != leader) {
                    nodes[j].clockDrift.set(-20_000 * ((i % 2 == 0) ? -1 : 1));
                }
            }
            AquariumNode newLeader = awaitLeader(mode, alive, rawRingSize);
            leader.clockDrift.set(0);
            leader = newLeader;
            for (int j = 0; j < running; j++) {
                nodes[j].clockDrift.set(0);
            }
        }

        mode = "Add 5 more nodes...";
        for (; running < nodes.length; running++) {
            rawLifecycles.compute(nodes[running].member, (member, value) -> (value != null) ? (value + 1) : 0);
            aliveFutures[running] = service.scheduleWithFixedDelay(nodes[running], 10, 10, TimeUnit.MILLISECONDS);
            rawRingSize.incrementAndGet();
            alive[running] = nodes[running];
            awaitLeader(mode, alive, rawRingSize);
        }

        mode = "Force each node to bootstrap...";
        for (int i = 0; i < running; i++) {
            nodes[i].forceCurrentState(State.bootstrap);
            awaitLeader(mode, alive, rawRingSize);
        }

        mode = "Force each node to inactive...";
        for (int i = 0; i < running; i++) {
            nodes[i].forceCurrentState(State.inactive);
            awaitLeader(mode, alive, rawRingSize);
        }

        mode = "Force each node to nominated...";
        for (int i = 0; i < running; i++) {
            nodes[i].forceCurrentState(State.nominated);
            awaitLeader(mode, alive, rawRingSize);
        }

        mode = "Force each node to demoted...";
        for (int i = 0; i < running; i++) {
            nodes[i].forceCurrentState(State.demoted);
            awaitLeader(mode, alive, rawRingSize);
        }

        mode = "Force each node to follower...";
        for (int i = 0; i < running; i++) {
            nodes[i].forceCurrentState(State.follower);
            awaitLeader(mode, alive, rawRingSize);
        }

        mode = "Force each node to leader...";
        for (int i = 0; i < running; i++) {
            nodes[i].forceCurrentState(State.leader);
            awaitLeader(mode, alive, rawRingSize);
        }

        mode = "Force last node to leader...";
        nodes[nodes.length - 1].forceDesiredState(State.leader);
        awaitLeader(mode, alive, rawRingSize);

        mode = "Expunge 1 nodes...";
        for (int i = 0; i < 1; i++) {
            rawLifecycles.compute(nodes[i].member, (member, value) -> (value != null) ? (value + 1) : 0);
            nodes[i].forceDesiredState(State.expunged);
            nodes[i].awaitDesiredState(State.expunged, nodes);
            if (aliveFutures[i].cancel(true)) {
                alive[i] = null;
                rawRingSize.decrementAndGet();
            }
            //awaitLeader(mode, alive, rawRingSize);
        }
        System.out.println("Expunged 1 node.");

        mode = "Re-add 1 nodes...";
        for (int i = 0; i < 1; i++) {
            nodes[i].clear();
            rawLifecycles.compute(nodes[i].member, (member, value) -> (value != null) ? (value + 1) : 0);
            nodes[i].forceDesiredState(State.leader);
            aliveFutures[i] = service.scheduleWithFixedDelay(nodes[i], 10, 10, TimeUnit.MILLISECONDS);
            rawRingSize.incrementAndGet();
            alive[i] = nodes[i];
            nodes[i].awaitDesiredState(State.leader, nodes);
            awaitLeader(mode, alive, rawRingSize);
        }

        mode = "Blow away node state...";
        for (int i = 0; i < running; i++) {
            nodes[i].clear();
            nodes[i].awaitCurrentState(State.leader, State.follower);
            awaitLeader(mode, alive, rawRingSize);
        }

        mode = "Change lifecycles...";
        for (int i = 0; i < running; i++) {
            rawLifecycles.compute(nodes[i].member, (member, value) -> (value != null) ? (value + 1) : 0);
            nodes[i].awaitCurrentState(State.leader, State.follower);
            awaitLeader(mode, alive, rawRingSize);
        }

        mode = "Change Force state";
        for (int i = 0; i < running; i++) {
            rawLifecycles.compute(nodes[i].member, (member, value) -> (value != null) ? (value + 1) : 0);
            nodes[i].awaitCurrentState(State.leader, State.follower);
            awaitLeader(mode, alive, rawRingSize);
        }

    }

    private void createNode(int i, Map<Member, Integer> rawLifecycles, NavigableMap<Key, TimestampedState<Void>> rawLiveliness,
        NavigableMap<Key, TimestampedState<State>> rawState, AtQuorum atQuorum, CurrentMembers currentMembers, int deadAfterMillis, AquariumNode[] nodes) {
        OrderIdProvider orderIdProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(i));

        Member member = new Member(intBytes(i));

        MemberLifecycle<Integer> memberLifecycle = member1 -> rawLifecycles.computeIfAbsent(member1, key -> 0);

        LivelinessStorage livelinessStorage = new LivelinessStorage() {
            @Override
            public boolean scan(Member rootMember, Member otherMember, LivelinessStorage.LivelinessStream stream) throws Exception {
                Member minA = (rootMember == null) ? MIN : rootMember;
                Member maxA = (rootMember == null) ? MAX : rootMember;
                Member minB = (otherMember == null) ? minA : otherMember;
                SortedMap<Key, TimestampedState<Void>> subMap = rawLiveliness.subMap(new Key(LIVELINESS, minA, 0, minB), new Key(LIVELINESS, maxA, 0, MAX));
                for (Map.Entry<Key, TimestampedState<Void>> e : subMap.entrySet()) {
                    Key key = e.getKey();
                    TimestampedState<Void> v = e.getValue();
                    if (!stream.stream(key.a, key.isSelf, key.b, v.timestamp, v.version)) {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public boolean update(LivelinessStorage.LivelinessUpdates updates) throws Exception {
                return updates.updates((rootMember, otherMember, timestamp) -> {
                    rawLiveliness.compute(new Key(LIVELINESS, rootMember, 0, otherMember), (key, myState) -> {
                        long version = orderIdProvider.nextId();
                        if (myState != null && (myState.timestamp > timestamp || (myState.timestamp == timestamp && myState.version > version))) {
                            return myState;
                        } else {
                            return new TimestampedState<>(null, timestamp, version);
                        }
                    });
                    return true;
                });
            }

            @Override
            public long get(Member rootMember, Member otherMember) throws Exception {
                TimestampedState<Void> timestampedState = rawLiveliness.get(new Key(LIVELINESS, rootMember, 0, otherMember));
                return timestampedState != null ? timestampedState.timestamp : -1;
            }
        };

        ContextualStateStorage currentStateStorage = new ContextualStateStorage(orderIdProvider, rawState, CURRENT);
        ContextualStateStorage desiredStateStorage = new ContextualStateStorage(orderIdProvider, rawState, DESIRED);

        AtomicLong clockDrift = new AtomicLong(0);
        CurrentTimeMillis currentTimeMillis = () -> System.currentTimeMillis() + clockDrift.get();
        AtomicLong firstLivelinessTimestamp = new AtomicLong(-1);
        Liveliness liveliness = new Liveliness(new AquariumStats(), currentTimeMillis, livelinessStorage, member, atQuorum, deadAfterMillis, firstLivelinessTimestamp);
        AtomicLong currentCount = new AtomicLong();
        TransitionQuorum ifYoureLuckyCurrentTransitionQuorum = (existing, nextTimestamp, nextState, readCurrent, readDesired, writeCurrent, writeDesired)
            -> {
            if (currentCount.incrementAndGet() % 2 == 0) {
                writeCurrent.put(existing.getMember(), nextState, nextTimestamp);
                return true;
            }
            return false;
        };
        AtomicLong desiredCount = new AtomicLong();
        TransitionQuorum ifYoureLuckyDesiredTransitionQuorum = (existing, nextTimestamp, nextState, readCurrent, readDesired, writeCurrent, writeDesired)
            -> {
            if (desiredCount.incrementAndGet() % 2 == 0) {
                writeDesired.put(existing.getMember(), nextState, nextTimestamp);
                return true;
            }
            return false;
        };
        nodes[i] = new AquariumNode(orderIdProvider,
            currentStateStorage,
            desiredStateStorage,
            member,
            firstLivelinessTimestamp,
            liveliness,
            memberLifecycle,
            atQuorum,
            currentMembers,
            ifYoureLuckyCurrentTransitionQuorum,
            ifYoureLuckyDesiredTransitionQuorum,
            clockDrift);
    }

    @Test
    public void testTapTheGlass() throws Exception {

        NavigableMap<Key, TimestampedState<State>> rawState = new ConcurrentSkipListMap<>();
        NavigableMap<Key, TimestampedState<Void>> rawLiveliness = new ConcurrentSkipListMap<>();

        AtomicInteger rawRingSize = new AtomicInteger();
        AtQuorum atQuorum = count -> count > rawRingSize.get() / 2;


        Map<Member, Integer> rawLifecycles = Maps.newConcurrentMap();
        CurrentMembers currentMembers = rawLifecycles::keySet;

        int aquariumNodeCount = 10;
        AquariumNode[] nodes = new AquariumNode[aquariumNodeCount];
        int deadAfterMillis = 10_000;
        for (int i = 0; i < aquariumNodeCount; i++) {
            createNode(i, rawLifecycles, rawLiveliness, rawState, atQuorum, currentMembers, deadAfterMillis, nodes);
        }

        ScheduledExecutorService service = Executors.newScheduledThreadPool(aquariumNodeCount);

        int running = 0;
        AquariumNode[] alive = new AquariumNode[aquariumNodeCount];
        Future[] aliveFutures = new Future[aquariumNodeCount];
        for (; running < 5; running++) {
            rawLifecycles.compute(nodes[running].member, (member, value) -> (value != null) ? (value + 1) : 0);
            aliveFutures[running] = service.scheduleWithFixedDelay(nodes[running], 10, 10, TimeUnit.MILLISECONDS);
            rawRingSize.incrementAndGet();
            alive[running] = nodes[running];
        }
        String mode = "Start with 5 nodes...";
        awaitLeader(mode, alive, rawRingSize);

        mode = "Force each node to be a leader...";
        AquariumNode leader = null;
        for (int i = 0; i < running; i++) {
            nodes[i].forceDesiredState(State.leader);
            leader = awaitLeader(mode, alive, rawRingSize);
        }

        mode = "Force each leader to follower and the back to leader...";
        for (int i = 0; i < running; i++) {

            leader.forceDesiredState(State.follower);
            leader.forceDesiredState(State.leader);
            leader = awaitLeader(mode, alive, rawRingSize);
        }

        mode = "Force all nodes to be a leader...";
        for (int i = 0; i < running; i++) {
            nodes[i].forceDesiredState(State.leader);
        }
        awaitLeader(mode, alive, rawRingSize);

        mode = "Force each node to be a follower...";
        for (int i = 0; i < running; i++) {
            if (i < running - 1) {
                nodes[i].forceDesiredState(State.follower);
            }
            leader = awaitLeader(mode, alive, rawRingSize);
        }

        mode = "Prove immune to clock drift by moving leader...";
        for (int i = 0; i < 2; i++) {
            leader.clockDrift.set(20_000 * ((i % 2 == 0) ? -1 : 1));
            AquariumNode newLeader = awaitLeader(mode, alive, rawRingSize);
            leader.clockDrift.set(0);
            leader = newLeader;
        }

        mode = "Prove immune to clock drift by moving followers...";
        for (int i = 0; i < running; i++) {
            for (int j = 0; j < running; j++) {
                if (nodes[j] != leader) {
                    nodes[j].clockDrift.set(-20_000 * ((i % 2 == 0) ? -1 : 1));
                }
            }
            AquariumNode newLeader = awaitLeader(mode, alive, rawRingSize);
            leader.clockDrift.set(0);
            leader = newLeader;
            for (int j = 0; j < running; j++) {
                nodes[j].clockDrift.set(0);
            }
        }

        mode = "Add 5 more nodes...";
        for (; running < nodes.length; running++) {
            rawLifecycles.compute(nodes[running].member, (member, value) -> (value != null) ? (value + 1) : 0);
            aliveFutures[running] = service.scheduleWithFixedDelay(nodes[running], 10, 10, TimeUnit.MILLISECONDS);
            rawRingSize.incrementAndGet();
            alive[running] = nodes[running];
            awaitLeader(mode, alive, rawRingSize);
        }

        mode = "Force each node to bootstrap...";
        for (int i = 0; i < running; i++) {
            nodes[i].forceCurrentState(State.bootstrap);
            awaitLeader(mode, alive, rawRingSize);
        }

        mode = "Force each node to inactive...";
        for (int i = 0; i < running; i++) {
            nodes[i].forceCurrentState(State.inactive);
            awaitLeader(mode, alive, rawRingSize);
        }

        mode = "Force each node to nominated...";
        for (int i = 0; i < running; i++) {
            nodes[i].forceCurrentState(State.nominated);
            awaitLeader(mode, alive, rawRingSize);
        }

        mode = "Force each node to demoted...";
        for (int i = 0; i < running; i++) {
            nodes[i].forceCurrentState(State.demoted);
            awaitLeader(mode, alive, rawRingSize);
        }

        mode = "Force each node to follower...";
        for (int i = 0; i < running; i++) {
            nodes[i].forceCurrentState(State.follower);
            awaitLeader(mode, alive, rawRingSize);
        }

        mode = "Force each node to leader...";
        for (int i = 0; i < running; i++) {
            nodes[i].forceCurrentState(State.leader);
            awaitLeader(mode, alive, rawRingSize);
        }

        mode = "Force last node to leader...";
        nodes[nodes.length - 1].forceDesiredState(State.leader);
        awaitLeader(mode, alive, rawRingSize);

        mode = "Expunge 5 nodes...";
        for (int i = 0; i < 5; i++) {
            rawLifecycles.compute(nodes[i].member, (member, value) -> (value != null) ? (value + 1) : 0);
            nodes[i].forceDesiredState(State.expunged);
            nodes[i].awaitDesiredState(State.expunged, nodes);
            if (aliveFutures[i].cancel(true)) {
                alive[i] = null;
                rawRingSize.decrementAndGet();
            }
            awaitLeader(mode, alive, rawRingSize);
        }

        mode = "Re-add 5 nodes...";
        for (int i = 0; i < 5; i++) {
            nodes[i].clear();
            rawLifecycles.compute(nodes[i].member, (member, value) -> (value != null) ? (value + 1) : 0);
            nodes[i].forceDesiredState(State.follower);
            aliveFutures[i] = service.scheduleWithFixedDelay(nodes[i], 10, 10, TimeUnit.MILLISECONDS);
            rawRingSize.incrementAndGet();
            alive[i] = nodes[i];
            nodes[i].awaitDesiredState(State.follower, nodes);
            awaitLeader(mode, alive, rawRingSize);
        }

        mode = "Blow away node state...";
        for (int i = 0; i < running; i++) {
            nodes[i].clear();
            nodes[i].awaitCurrentState(State.leader, State.follower);
            awaitLeader(mode, alive, rawRingSize);
        }

        mode = "Force node to leader...";
        nodes[0].forceDesiredState(State.leader);
        leader = awaitLeader(mode, alive, rawRingSize);
        Assert.assertNotNull(leader);

        for (int i = 0; i < running; i++) {
            Assert.assertTrue(nodes[0].aquarium.isLivelyEndState(nodes[i].member));
            Assert.assertTrue(nodes[0].aquarium.isLivelyState(nodes[i].member, i == 0 ? State.leader : State.follower));
        }

        mode = "Force leader to demoted...";
        nodes[0].forceCurrentState(State.demoted);
        long currentTimestamp = nodes[0].aquarium.livelyEndState().getCurrentWaterline().getTimestamp();
        long currentVersion = nodes[0].aquarium.livelyEndState().getCurrentWaterline().getVersion();

        awaitLeader(mode, alive, rawRingSize);
        long updatedTimestamp = nodes[0].aquarium.livelyEndState().getCurrentWaterline().getTimestamp();
        long updatedVersion = nodes[0].aquarium.livelyEndState().getCurrentWaterline().getVersion();

        // current version should have advanced when it re-achieved leader
        Assert.assertEquals(updatedTimestamp, currentTimestamp);
        Assert.assertTrue(updatedVersion > currentVersion);

        for (int i = 0; i < running; i++) {
            Assert.assertTrue(leader.aquarium.isLivelyEndState(nodes[i].member));
            Assert.assertTrue(leader.aquarium.isLivelyState(nodes[i].member, i == 0 ? State.leader : State.follower)); // hmmm
        }

    }

    private AquariumNode awaitLeader(String mode, AquariumNode[] nodes, AtomicInteger ringSize) throws Exception {

        AquariumNode leader = null;
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < 2_000_000) {

            int follower = 0;
            int leaders = 0;
            for (AquariumNode node : nodes) {
                if (node == null) {
                    continue;
                }
                LivelyEndState awaitOnline = node.aquarium.awaitOnline(1000);
                LivelyEndState livelyEndState = node.aquarium.livelyEndState();
                if (livelyEndState != null) {
                    if (livelyEndState.isOnline()) {
                        if (livelyEndState.getCurrentState() == State.follower) {

                            Waterline leaderWaterLine = node.aquarium.getLeader();
                            Assert.assertTrue(Waterline.checkEquals(leaderWaterLine, livelyEndState.getLeaderWaterline()));

                            follower++;

                        }
                        if (livelyEndState.getCurrentState() == State.leader) {
                            leader = node;
                            leaders++;
                        }
                    }
                }
                node.printState(mode);
            }

            if (leaders == 1 && (leaders + follower == ringSize.get())) {
                //Assert.assertTrue(leader.aquarium.isLivelyState(leader.member, State.leader));
                //Assert.assertTrue(leader.aquarium.isLivelyEndState(leader.member));

                System.out.println("<<<<<<<<<<<<<<<<<<<< Hooray >>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                return leader;
            } else {
                System.out.println("----------------------------------------");
                Thread.sleep(100);
            }
        }
        Assert.fail();
        return null;
    }

    class AquariumNode implements Runnable {

        private final OrderIdProvider orderIdProvider;
        private final ContextualStateStorage currentStateStorage;
        private final ContextualStateStorage desiredStateStorage;
        private final Member member;
        private final AtomicLong firstLivelinessTimestamp;
        private final Liveliness liveliness;
        private final AtomicLong clockDrift;

        private final Aquarium aquarium;

        public AquariumNode(OrderIdProvider orderIdProvider,
            ContextualStateStorage currentStateStorage,
            ContextualStateStorage desiredStateStorage,
            Member member,
            AtomicLong firstLivelinessTimestamp,
            Liveliness liveliness,
            MemberLifecycle<Integer> memberLifecycle,
            AtQuorum atQuorum,
            CurrentMembers currentMembers,
            TransitionQuorum ifYoureLuckyCurrentTransitionQuorum,
            TransitionQuorum ifYoureLuckyDesiredTransitionQuorum,
            AtomicLong clockDrift) {

            this.orderIdProvider = orderIdProvider;
            this.currentStateStorage = currentStateStorage;
            this.desiredStateStorage = desiredStateStorage;
            this.member = member;
            this.firstLivelinessTimestamp = firstLivelinessTimestamp;
            this.liveliness = liveliness;
            this.clockDrift = clockDrift;

            this.aquarium = new Aquarium(new AquariumStats(),
                orderIdProvider,
                currentStateStorage,
                desiredStateStorage,
                ifYoureLuckyCurrentTransitionQuorum,
                ifYoureLuckyDesiredTransitionQuorum,
                liveliness,
                memberLifecycle,
                Integer.class,
                atQuorum,
                currentMembers,
                member,
                new AwaitLivelyEndState() {
                    @Override
                    public LivelyEndState awaitChange(Callable<LivelyEndState> awaiter, long timeoutMillis) throws Exception {
                        return awaiter.call();
                    }

                    @Override
                    public void notifyChange(Callable<Boolean> change) throws Exception {
                        change.call();
                    }
                });
        }

        public void forceDesiredState(State state) throws Exception {
            aquarium.tx((readCurrent, readDesired, writeCurrent, writeDesired) -> {
                Waterline currentWaterline = readCurrent.get(member);
                if (currentWaterline == null) {
                    currentWaterline = new Waterline(member, State.bootstrap, orderIdProvider.nextId(), -1L, true);
                }
                System.out.println("FORCING DESIRED " + state + ":" + member);
                writeDesired.put(member, state, orderIdProvider.nextId());
                return null;
            });
        }

        public void forceCurrentState(State state) throws Exception {
            aquarium.tx((readCurrent, readDesired, writeCurrent, writeDesired) -> {
                Waterline currentWaterline = readCurrent.get(member);
                if (currentWaterline == null) {
                    currentWaterline = new Waterline(member, State.bootstrap, orderIdProvider.nextId(), -1L, true);
                }
                Waterline desiredWaterline = readDesired.get(member);
                if (desiredWaterline != null) {
                    System.out.println("FORCING CURRENT " + state + ":" + member);
                    writeCurrent.put(member, state, desiredWaterline.getTimestamp());
                }
                return true;
            });
        }

        public void awaitCurrentState(State... states) throws Exception {
            Set<State> acceptable = Sets.newHashSet(states);
            boolean[] reachedCurrent = { false };
            while (!reachedCurrent[0]) {
                aquarium.tx((readCurrent, readDesired, writeCurrent, writeDesired) -> {
                    Waterline currentWaterline = readCurrent.get(member);
                    if (currentWaterline != null) {
                        reachedCurrent[0] = acceptable.contains(currentWaterline.getState()) && currentWaterline.isAtQuorum();
                    }
                    return true;
                });
                System.out.println(member + " awaitCurrentState " + Arrays.toString(states));
                Thread.sleep(100);
            }
        }

        public void awaitDesiredState(State state, AquariumNode[] nodes) throws Exception {
            boolean[] reachedDesired = { false };
            while (!reachedDesired[0]) {
                Waterline[] currentWaterline = { null };
                aquarium.tx((readCurrent, readDesired, writeCurrent, writeDesired) -> {
                    currentWaterline[0] = readCurrent.get(member);
                    if (currentWaterline[0] != null) {
                        Waterline desiredWaterline = readDesired.get(member);

                        reachedDesired[0] = currentWaterline[0].getState() == state
                            && currentWaterline[0].isAtQuorum()
                            && Waterline.checkEquals(currentWaterline[0], desiredWaterline);
                    }
                    return true;
                });
                for (AquariumNode node : nodes) {
                    if (node != null) {
                        node.printState("awaitDesiredState " + state + " for " + (node == this ? "me" : member));
                    }
                }

                Thread.sleep(100);
            }
        }

        @Override
        public void run() {
            try {
                liveliness.feedTheFish();
                aquarium.acknowledgeOther();
                aquarium.tapTheGlass();
            } catch (Exception x) {
                x.printStackTrace();
            }
        }

        private void clear() {
            firstLivelinessTimestamp.set(-1);
            currentStateStorage.clear(member);
            desiredStateStorage.clear(member);
        }

        private void printState(String mode) throws Exception {
            aquarium.tx((readCurrent, readDesired, writeCurrent, writeDesired) -> {
                Waterline currentWaterline = readCurrent.get(member);
                Waterline desiredWaterline = readDesired.get(member);
                System.out.println(bytesInt(member.getMember()) + " " + mode);
                System.out.println("\tCurrent:" + currentWaterline);
                System.out.println("\tDesired:" + desiredWaterline);
                return true;
            });
        }

    }

    private static class ContextualStateStorage implements StateStorage<Integer> {

        private final OrderIdProvider orderIdProvider;
        private final NavigableMap<Key, TimestampedState<State>> stateStorage;
        private final byte context;

        public ContextualStateStorage(OrderIdProvider orderIdProvider,
            NavigableMap<Key, TimestampedState<State>> stateStorage,
            byte context) {
            this.orderIdProvider = orderIdProvider;
            this.stateStorage = stateStorage;
            this.context = context;
        }

        @Override
        public boolean scan(Member rootMember, Member otherMember, Integer lifecycle, StateStream<Integer> stream) throws Exception {
            Member minA = (rootMember == null) ? MIN : rootMember;
            Member maxA = (rootMember == null) ? MAX : rootMember;
            Member minB = (otherMember == null) ? minA : otherMember;
            int minLifecycle = (lifecycle != null) ? lifecycle : Integer.MAX_VALUE; // reversed
            int maxLifecycle = (lifecycle != null) ? lifecycle : Integer.MIN_VALUE; // reversed
            SortedMap<Key, TimestampedState<State>> subMap = stateStorage.subMap(new Key(context, minA, minLifecycle, minB),
                new Key(context, maxA, maxLifecycle, MAX));
            for (Map.Entry<Key, TimestampedState<State>> e : subMap.entrySet()) {
                Key key = e.getKey();
                TimestampedState<State> v = e.getValue();
                if (!stream.stream(key.a, key.isSelf, key.b, key.memberVersion, v.state, v.timestamp, v.version)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public boolean update(StateUpdates<Integer> updates) throws Exception {
            return updates.updates((rootMember, otherMember, lifecycle, state, timestamp) -> {
                stateStorage.compute(new Key(context, rootMember, lifecycle, otherMember), (key, myState) -> {
                    long version = orderIdProvider.nextId();
                    if (myState != null && (myState.timestamp > timestamp || (myState.timestamp == timestamp && myState.version > version))) {
                        return myState;
                    } else {
                        return new TimestampedState<>(state, timestamp, version);
                    }
                });
                return true;
            });
        }

        public void clear(Member rootMember) {

            SortedMap<Key, TimestampedState<State>> subMap = stateStorage.subMap(new Key(context, rootMember, Integer.MAX_VALUE, rootMember),
                new Key(context, rootMember, Integer.MIN_VALUE, MAX));
            subMap.clear();
        }
    }

    static class Key implements Comparable<Key> {

        final byte context; // current = 0, desired = 1;
        final Member a;
        final int memberVersion;
        final boolean isSelf;
        final Member b;

        public Key(byte context, Member a, int memberVersion, Member b) {
            this.context = context;
            this.a = a;
            this.memberVersion = memberVersion;
            this.isSelf = a.equals(b);
            this.b = b;

        }

        @Override
        public int hashCode() {
            throw new UnsupportedOperationException("stop");
        }

        @Override
        public boolean equals(Object obj) {
            throw new UnsupportedOperationException("stop");
        }

        @Override
        public String toString() {
            return "Key{"
                + "context=" + context
                + ", a=" + a
                + ", memberVersion=" + memberVersion
                + ", isSelf=" + isSelf
                + ", b=" + b
                + '}';
        }

        @Override
        public int compareTo(Key o) {
            int c = Byte.compare(context, o.context);
            if (c != 0) {
                return c;
            }
            c = UnsignedBytes.lexicographicalComparator().compare(a.getMember(), o.a.getMember());
            if (c != 0) {
                return c;
            }
            c = -Integer.compare(memberVersion, o.memberVersion);
            if (c != 0) {
                return c;
            }
            c = -Boolean.compare(isSelf, o.isSelf);
            if (c != 0) {
                return c;
            }
            c = UnsignedBytes.lexicographicalComparator().compare(b.getMember(), o.b.getMember());
            if (c != 0) {
                return c;
            }
            return c;
        }

    }

    static class TimestampedState<S> {

        final S state;
        final long timestamp;
        final long version;

        public TimestampedState(S state, long timestamp, long version) {
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

    static byte[] intBytes(int v) {
        return intBytes(v, new byte[4], 0);
    }

    static byte[] intBytes(int v, byte[] _bytes, int _offset) {
        _bytes[_offset + 0] = (byte) (v >>> 24);
        _bytes[_offset + 1] = (byte) (v >>> 16);
        _bytes[_offset + 2] = (byte) (v >>> 8);
        _bytes[_offset + 3] = (byte) v;
        return _bytes;
    }

    static int bytesInt(byte[] _bytes) {
        return bytesInt(_bytes, 0);
    }

    static int bytesInt(byte[] bytes, int _offset) {
        int v = 0;
        v |= (bytes[_offset + 0] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 1] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 2] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 3] & 0xFF);
        return v;
    }
}
