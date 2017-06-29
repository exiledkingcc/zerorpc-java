package org.zerorpc;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

import java.io.ByteArrayOutputStream;
import java.io.IOException;


public class ZRpcClient {
    private static final int HEARTBEAT = 5;
    private static final int TIMEOUT = 30;

    /** TODO **/
    private final int heartbeat;
    private final int timeout;
    private boolean async = false;

    private ZContext context;
    private Socket socket;

    public ZRpcClient(int heartbeat, int timeout) {
        this.heartbeat = heartbeat;
        this.timeout = timeout;
        this.context = new ZContext();
        this.socket = context.createSocket(ZMQ.REQ);
    }

    public ZRpcClient() {
        this(HEARTBEAT, TIMEOUT);
    }

    public void connect(String address) {
        this.socket.connect(address);
    }

    public <T> T call(String command, Object... args) throws IOException {
        final Event event = Event.newRequest(command, args);
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        event.serialize(out);

        this.socket.send(out.toByteArray());
        byte[] response = this.socket.recv();
        final Event eventx = Event.deserialize(response);
        Object o = eventx.getResult();
        return (T)o;
    }

    public void asyncCall(String command, Object... args) throws IOException {
        final Event event = Event.newRequest(command, args);
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        event.serialize(out);

        this.socket.send(out.toByteArray());
        this.async = true;
    }

    public <T> T get(boolean blocking) throws IOException {
        if (!async) {
            throw new IllegalStateException("ZRpcClient.get should be only used after asyncCall");
        }

        byte[] response = null;
        if (blocking) {
            response = this.socket.recv();
        } else {
            response = this.socket.recv(ZMQ.DONTWAIT);
        }

        if (response != null) {
            async = false;
            final Event event = Event.deserialize(response);
            Object o = event.getResult();
            return (T) o;
        } else {
            return null;
        }
    }

    public <T> T get() throws IOException {
        return this.get(true);
    }
}