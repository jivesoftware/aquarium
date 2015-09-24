package com.jivesoftware.os.aquarium;

import com.google.common.collect.Sets;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class Liveliness {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final CurrentTimeMillis currentTimeMillis;
    private final LivelinessStorage livelinessStorage;
    private final Member member;
    private final AtQuorum atQuorum;
    private final long deadAfterMillis;
    private final AtomicLong firstLivelinessTimestamp;

    private final AtomicLong myAliveUntilTimestamp = new AtomicLong(-1);
    private final ConcurrentHashMap<Member, Long> otherAliveUntilTimestamp = new ConcurrentHashMap<>();

    public Liveliness(CurrentTimeMillis currentTimeMillis,
        LivelinessStorage livelinessStorage,
        Member member,
        AtQuorum atQuorum,
        long deadAfterMillis,
        AtomicLong firstLivelinessTimestamp) {
        this.currentTimeMillis = currentTimeMillis;
        this.livelinessStorage = livelinessStorage;
        this.atQuorum = atQuorum;
        this.member = member;
        this.deadAfterMillis = deadAfterMillis;
        this.firstLivelinessTimestamp = firstLivelinessTimestamp;
    }

    public void feedTheFish() throws Exception {
        blowBubbles();
        acknowledgeOther();
    }

    private void blowBubbles() throws Exception {
        /*LOG.info("Blowing bubbles...");*/
        long timestamp = currentTimeMillis.get();
        livelinessStorage.update(setLiveliness -> setLiveliness.set(member, member, timestamp));
        firstLivelinessTimestamp.compareAndSet(-1, timestamp);
        /*LOG.info("Blew bubbles in {}", (currentTimeMillis.get() - timestamp));*/
    }

    private void acknowledgeOther() throws Exception {
        /*LOG.info("Acknowledging others...");
        long start = currentTimeMillis.get();*/

        long[] myAliveCurrentTimestamp = { -1L };
        long[] myAliveLatestAck = { -1 };
        Set<Member> myAliveAcked = Sets.newHashSet();

        livelinessStorage.update(setLiveliness -> {
            LivelinessEntry[] ackOtherE = new LivelinessEntry[1];
            boolean[] ackOtherColdstart = { true };

            //byte[] fromKey = stateKey(versionedPartitionName.getPartitionName(), context, versionedPartitionName.getPartitionVersion(), null, null);
            livelinessStorage.scan(null, null, (rootMember, isSelf, ackMember, timestamp, version) -> {
                if (ackOtherE[0] != null && !ackOtherE[0].rootMember.equals(rootMember)) {
                    if (ackOtherColdstart[0]) {
                        setLiveliness.set(ackOtherE[0].rootMember, member, ackOtherE[0].timestamp);
                    }
                    ackOtherE[0] = null;
                    ackOtherColdstart[0] = true;
                }

                if (ackOtherE[0] == null && isSelf && !member.equals(rootMember)) {
                    ackOtherE[0] = new LivelinessEntry(rootMember, timestamp);
                }
                if (ackOtherE[0] != null && member.equals(ackMember)) {
                    ackOtherColdstart[0] = false;
                    if (timestamp != ackOtherE[0].timestamp) {
                        setLiveliness.set(ackOtherE[0].rootMember, member, ackOtherE[0].timestamp);
                    }
                }

                if (rootMember.equals(member)) {
                    if (myAliveCurrentTimestamp[0] == -1L && isSelf) {
                        myAliveCurrentTimestamp[0] = timestamp;
                        myAliveAcked.add(ackMember);
                    } else if (myAliveCurrentTimestamp[0] != -1L) {
                        if (timestamp >= (myAliveCurrentTimestamp[0] - deadAfterMillis)) {
                            myAliveLatestAck[0] = Math.max(myAliveLatestAck[0], timestamp);
                            myAliveAcked.add(ackMember);
                        }
                    }

                    if (!isSelf) {
                        otherAliveUntilTimestamp.put(ackMember, timestamp + deadAfterMillis);
                    }
                }

                return true;
            });

            if (ackOtherE[0] != null && ackOtherColdstart[0]) {
                setLiveliness.set(ackOtherE[0].rootMember, member, ackOtherE[0].timestamp);
            }
            return true;
        });

        if (myAliveCurrentTimestamp[0] != -1L && atQuorum.is(myAliveAcked.size())) {
            myAliveUntilTimestamp.set(myAliveLatestAck[0] + deadAfterMillis);
        } else {
            myAliveUntilTimestamp.set(-1);
        }

        /*LOG.info("Acknowledged others in {}", (currentTimeMillis.get() - start));*/
    }

    public long aliveUntilTimestamp(Member asMember) throws Exception {
        if (member.equals(asMember)) {
            return myAliveUntilTimestamp();
        } else {
            return otherAliveUntilTimestamp(asMember);
        }
    }

    public boolean isAlive(Member asMember) throws Exception {
        return currentTimeMillis.get() <= aliveUntilTimestamp(asMember);
    }

    private long myAliveUntilTimestamp() throws Exception {
        if (deadAfterMillis <= 0) {
            return Long.MAX_VALUE;
        }

        return myAliveUntilTimestamp.get();
    }

    private long otherAliveUntilTimestamp(Member other) throws Exception {
        if (deadAfterMillis <= 0) {
            return Long.MAX_VALUE;
        }

        long firstTimestamp = firstLivelinessTimestamp.get();
        if (firstTimestamp < 0) {
            return Long.MAX_VALUE;
        }

        return otherAliveUntilTimestamp.getOrDefault(other, firstTimestamp + deadAfterMillis);
    }

    private static class LivelinessEntry {

        private final Member rootMember;
        private final long timestamp;

        public LivelinessEntry(Member rootMember,
            long timestamp) {
            this.rootMember = rootMember;
            this.timestamp = timestamp;
        }
    }

}
