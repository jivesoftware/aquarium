package com.jivesoftware.os.aquarium;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class LivelyEndStateNGTest {

    @Test
    public void testGetters() throws Exception {

        Waterline currentWaterline = new Waterline(new Member(new byte[]{1}), State.demoted, 1, 0, true);
        Waterline desiredWaterline = new Waterline(new Member(new byte[]{1}), State.leader, 2, 0, true);
        Waterline leaderWaterline = new Waterline(new Member(new byte[]{2}), State.leader, 1, 0, true);

        LivelyEndState state = new LivelyEndState(null, currentWaterline, desiredWaterline, leaderWaterline);
        Assert.assertTrue(state.getCurrentWaterline() == currentWaterline);
        Assert.assertTrue(state.getLeaderWaterline() == leaderWaterline);
        Assert.assertEquals(state.getCurrentState(), State.demoted);
        Assert.assertEquals(state.isOnline(), true);


        state = new LivelyEndState((Member member) -> true, currentWaterline, desiredWaterline, leaderWaterline);
        Assert.assertEquals(state.isOnline(), false);

        state = new LivelyEndState((Member member) -> false, currentWaterline, desiredWaterline, leaderWaterline);
        Assert.assertEquals(state.isOnline(), false);


        currentWaterline = new Waterline(new Member(new byte[]{1}), State.leader, 2, 0, true);
        state = new LivelyEndState((Member member) -> true, currentWaterline, desiredWaterline, leaderWaterline);
        Assert.assertEquals(state.isOnline(), true);

        state = new LivelyEndState((Member member) -> false, currentWaterline, desiredWaterline, leaderWaterline);
        Assert.assertEquals(state.isOnline(), false);

        System.out.println(state.toString());
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testNoEquals() {
        LivelyEndState state = new LivelyEndState(null, Waterline.ALWAYS_ONLINE, Waterline.ALWAYS_ONLINE, Waterline.ALWAYS_ONLINE);
        state.equals(null);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testNoHashCode() {
        LivelyEndState state = new LivelyEndState(null, Waterline.ALWAYS_ONLINE, Waterline.ALWAYS_ONLINE, Waterline.ALWAYS_ONLINE);
        state.hashCode();
    }

}
