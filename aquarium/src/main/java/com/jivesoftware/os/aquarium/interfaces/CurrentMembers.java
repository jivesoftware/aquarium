package com.jivesoftware.os.aquarium.interfaces;

import com.jivesoftware.os.aquarium.Member;
import java.util.Set;

/**
 *
 */
public interface CurrentMembers {

    Set<Member> getCurrent() throws Exception;
}
