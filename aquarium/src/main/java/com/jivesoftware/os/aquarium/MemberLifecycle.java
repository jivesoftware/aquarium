package com.jivesoftware.os.aquarium;

/**
 *
 */
public interface MemberLifecycle<T> {

    T get() throws Exception;

    T getOther(Member other) throws Exception;
}
