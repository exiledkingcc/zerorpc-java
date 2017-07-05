package org.zerorpc;

import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

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
        this.socket = context.createSocket(ZMQ.DEALER);
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
        Object o = null;
        while (o == null) {
            ZMsg msg = ZMsg.recvMsg(this.socket);
            ZFrame iden = msg.pop();
            ZFrame data = msg.pop();
            msg.destroy();

            final Event eventx = Event.deserialize(data.getData());
            if (eventx.getMethod().equals("_zpc_hb")) {
                continue;
            }
            o = eventx.getResult();
        }
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

        if (blocking) {
            Object o = null;
            while (o == null) {
                ZMsg msg = ZMsg.recvMsg(this.socket);
                ZFrame iden = msg.pop();
                ZFrame data = msg.pop();
                msg.destroy();

                final Event eventx = Event.deserialize(data.getData());
                if (eventx.getMethod().equals("_zpc_hb")) {
                    continue;
                }
                o = eventx.getResult();
            }
            return (T)o;
        } else {
            final byte[] response = this.socket.recv(ZMQ.DONTWAIT);
            if (response == null) {
                return null;
            }
            ZMsg msg = ZMsg.recvMsg(this.socket);
            ZFrame iden = msg.pop();
            ZFrame data = msg.pop();
            msg.destroy();

            final Event event = Event.deserialize(data.getData());
            if (event.getMethod().equals("_zpc_hb")) {
                return null;
            }
            return (T)event.getResult();
        }
    }

    public <T> T get() throws IOException {
        return this.get(true);
    }

}