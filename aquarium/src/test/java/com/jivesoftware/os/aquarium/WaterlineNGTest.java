package com.jivesoftware.os.aquarium;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class WaterlineNGTest {

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testNoEquals() {
        Waterline waterline = new Waterline(new Member(new byte[]{1}), State.leader, 0, 0, true);
        waterline.equals(null);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testNoHashCode() {
        Waterline waterline = new Waterline(new Member(new byte[]{1}), State.leader, 0, 0, true);
        waterline.hashCode();
    }

    @Test
    public void testCompare() {
        Waterline a = new Waterline(new Member(new byte[]{1}), State.leader, 1, 1, true);
       
        Assert.assertEquals(Waterline.compare(a, a), 0);
        Assert.assertEquals(Waterline.compare(a, new Waterline(new Member(new byte[]{1}), State.leader, 1, 1, true)), 0);
        Assert.assertEquals(Waterline.compare(a, new Waterline(new Member(new byte[]{2}), State.leader, 1, 1, true)), -1);
        Assert.assertEquals(Waterline.compare(a, new Waterline(new Member(new byte[]{1}), State.bootstrap, 1, 1, true)), 3);
        Assert.assertEquals(Waterline.compare(a, new Waterline(new Member(new byte[]{1}), State.leader, 2, 1, true)), 1);
        Assert.assertEquals(Waterline.compare(a, new Waterline(new Member(new byte[]{1}), State.leader, 0, 1, true)), -1);
        Assert.assertEquals(Waterline.compare(a, new Waterline(new Member(new byte[]{1}), State.leader, 1, 0, true)), -1);
        Assert.assertEquals(Waterline.compare(a, new Waterline(new Member(new byte[]{1}), State.leader, 1, 2, true)), 1);

        Waterline b = new Waterline(new Member(new byte[]{1}), State.leader, 0, 0, false);
        Assert.assertEquals(Waterline.compare(b, new Waterline(new Member(new byte[]{1}), State.leader, 0, 0, true)), 1);
    }

    @Test
    public void testEquals() {
        Waterline a = new Waterline(new Member(new byte[]{1}), State.leader, 0, 0, true);

        Assert.assertEquals(Waterline.checkEquals(a, a), true);
        Assert.assertEquals(Waterline.checkEquals(a, new Waterline(new Member(new byte[]{1}), State.leader, 0, 0, true)), true);
        Assert.assertEquals(Waterline.checkEquals(a, new Waterline(new Member(new byte[]{2}), State.leader, 0, 0, true)), false);
        Assert.assertEquals(Waterline.checkEquals(a, new Waterline(new Member(new byte[]{1}), State.bootstrap, 0, 0, true)), false);
        Assert.assertEquals(Waterline.checkEquals(a, new Waterline(new Member(new byte[]{1}), State.leader, 1, 0, true)), false);
        Assert.assertEquals(Waterline.checkEquals(a, new Waterline(new Member(new byte[]{1}), State.leader, 0, 1, true)), true);
        Assert.assertEquals(Waterline.checkEquals(a, new Waterline(new Member(new byte[]{1}), State.leader, 0, 0, false)), false);

        
    }
}
