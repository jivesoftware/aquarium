package com.jivesoftware.os.aquarium;

import com.google.common.primitives.UnsignedBytes;
import java.util.Arrays;

/**
 *
 * @author jonathan.colt
 */
public class Member implements Comparable<Member> {

    private final byte[] member;

    public Member(byte[] member) {
        this.member = member;
    }

    public byte[] getMember() {
        return member;
    }

    @Override
    public int compareTo(Member o) {
        return UnsignedBytes.lexicographicalComparator().compare(member, o.member);
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 89 * hash + Arrays.hashCode(this.member);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        final Member other = (Member) obj;
        return Arrays.equals(this.member, other.member);
    }

    @Override
    public String toString() {
        return "Member{" + "member=" + Arrays.toString(member) + '}';
    }

}
