package com.jivesoftware.os.aquarium.interfaces;

import com.jivesoftware.os.aquarium.Member;

/**
 *
 */
public interface IsCurrentMember {

    boolean isCurrent(Member member) throws Exception;
}
