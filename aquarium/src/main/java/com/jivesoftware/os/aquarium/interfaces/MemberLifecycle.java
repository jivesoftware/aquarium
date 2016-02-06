package com.jivesoftware.os.aquarium.interfaces;

import com.jivesoftware.os.aquarium.Member;

/**
 *
 */
public interface MemberLifecycle<T> {

    T get(Member member) throws Exception;
}
