package org.zerorpc;

import org.msgpack.value.Value;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import java.util.concurrent.ConcurrentLinkedQueue;


public class ZRpcWorker<THandler> implements Runnable {
    private THandler handler;

    private ConcurrentLinkedQueue<ZMessage> requestqueue;
    private ConcurrentLinkedQueue<ZMessage> replayqueue;

    ZRpcWorker(ConcurrentLinkedQueue<ZMessage> requestqueue, ConcurrentLinkedQueue<ZMessage> replayqueue, Class<THandler> handlerClass) {
        this.requestqueue = requestqueue;
        this.replayqueue = replayqueue;

        try {
            this.handler = handlerClass.newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    private Object invoke(Event event) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        /** handle heartbeat * */
        String method = event.getMethod();
        if (method.equals("_zpc_hb")) {
            return null;
        }
        Value[] args = event.getArgs();
        if(args != null && args.length > 0) {
            final Class<?>[] types = new Class<?>[args.length];
            final Object[] values = new Object[args.length];
            for(int i = 0; i < args.length; i++) {
                values[i] = Convert.toObject(args[i]);
                types[i] = values[i].getClass();

            }
            Method m = this.handler.getClass().getMethod(method, types);
            return m.invoke(this.handler, values);
        } else {
            Method m = this.handler.getClass().getMethod(method);
            return m.invoke(this.handler);
        }
    }

    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                final ZMessage req =  this.requestqueue.poll();
                if (req == null) {
                    Thread.sleep(100);
                    continue;
                }

                final Event event = Event.deserialize(req.getData());
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                try {
                    event.serialize(out, invoke(event));
                } catch (Exception e) {
                    e.printStackTrace();
                    event.serialize(out, e);
                }

                replayqueue.add(new ZMessage(req.getIdentity(), out.toByteArray()));
            } catch (Exception e) {
                e.printStackTrace();
                break;
            }
        }
    }
}