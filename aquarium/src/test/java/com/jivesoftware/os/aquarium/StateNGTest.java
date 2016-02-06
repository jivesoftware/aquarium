package com.jivesoftware.os.aquarium;

import junit.framework.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class StateNGTest {

    @Test
    public void testSerializedForm() {
        for (State value : State.values()) {
            byte serializedForm = value.getSerializedForm();
            Assert.assertEquals(State.fromSerializedForm(serializedForm), value);
        }

        Assert.assertEquals(State.fromSerializedForm((byte) -1), null);
    }

}
