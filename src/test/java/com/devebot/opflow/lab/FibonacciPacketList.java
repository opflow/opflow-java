package com.devebot.opflow.lab;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author drupalex
 */
public class FibonacciPacketList {
    private final List<FibonacciPacket> list = new ArrayList<FibonacciPacket>();

    public FibonacciPacketList() {
    }

    public FibonacciPacketList(List<FibonacciPacket> init) {
        list.addAll(init);
    }

    public List<FibonacciPacket> getList() {
        List<FibonacciPacket> copied = new ArrayList<FibonacciPacket>();
        copied.addAll(list);
        return copied;
    }

    public void add(FibonacciPacket packet) {
        list.add(packet);
    }
}
