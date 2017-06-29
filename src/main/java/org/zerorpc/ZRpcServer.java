package org.zerorpc;

import org.zeromq.ZContext;

import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

import java.util.concurrent.ConcurrentLinkedQueue;


public class ZRpcServer<THandler> {
    private static int THREAD_NUM = 4;
    private int numThreads = THREAD_NUM;
    private ZContext context;
    private Socket socket;
    private Class<THandler> handlerClass;
    private ConcurrentLinkedQueue<ZMessage> requestqueue;
    private ConcurrentLinkedQueue<ZMessage> replayqueue;

    public ZRpcServer(String address, Class<THandler> handlerClass, int numThreads) {
        this.handlerClass = handlerClass;
        this.numThreads = numThreads;

        this.context = new ZContext();
        this.socket = this.context.createSocket(ZMQ.ROUTER);
        socket.bind(address);

        this.requestqueue = new ConcurrentLinkedQueue<>();
        this.replayqueue = new ConcurrentLinkedQueue<>();
    }

    public ZRpcServer(String address, Class<THandler> handlerClass) {
        this(address, handlerClass, THREAD_NUM);
    }

    public void run() {
        for (int i = 0; i < this.numThreads; i++) {
            new Thread(new ZRpcWorker<>(this.requestqueue, this.replayqueue, this.handlerClass)).start();
        }
        new Thread(replayThread).start();

        while (!Thread.currentThread().isInterrupted()) {
            ZMsg msg = ZMsg.recvMsg(this.socket);
            ZFrame iden = msg.pop();
            ZFrame deli = msg.pop();
            ZFrame data = msg.pop();
            msg.destroy();

            final ZMessage req = new ZMessage(iden.getData(), data.getData());
            this.requestqueue.add(req);
        }
        this.socket.close();
        this.context.destroy();
    }

    public void stop() {
        this.socket.close();
        this.context.destroy();
    }

    private Runnable replayThread = new Runnable() {
        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                final ZMessage req = replayqueue.poll();
                if (req == null) {
                    continue;
                }
                ZMsg zmg = new ZMsg();
                zmg.add(req.getIdentity());
                zmg.add(new byte[]{});
                zmg.add(req.getData());
                zmg.send(socket);
            }
        }
    };
}