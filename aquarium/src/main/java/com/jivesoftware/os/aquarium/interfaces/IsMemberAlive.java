package com.jivesoftware.os.aquarium.interfaces;

import com.jivesoftware.os.aquarium.Member;

/**
 *
 * @author jonathan.colt
 */
public interface IsMemberAlive {

    boolean isAlive(Member member) throws Exception;
}
