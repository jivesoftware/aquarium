package com.jivesoftware.os.aquarium;

/**
 *
 */
public interface MemberLifecycle<T> {

    T get(Member member) throws Exception;
}
