package org.zerorpc;

class ZMessage {
    private byte[] identity;
    private byte[] data;

    ZMessage(byte[] identity, byte[] data) {
        this.identity = identity;
        this.data = data;
    }

    byte[] getIdentity() {
        return this.identity;
    }

    byte[] getData() {
        return this.data;
    }
}