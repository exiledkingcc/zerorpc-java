package org.zerorpc;

import org.junit.Test;

import java.io.IOException;


public class ZTest {
    @Test
    public void testSever() {
        final ZRpcServer<RPCHandler> server = new ZRpcServer<>("tcp://0.0.0.0:12345", RPCHandler.class);
        server.run();
    }

    @Test
    public void testClient() {
        Thread t1 = new Thread(new Client(400));
        Thread t2 = new Thread(new Client(300));
        Thread t3 = new Thread(new Client(200));
        Thread t4 = new Thread(new Client(100));

        try {
            t1.start();
            t2.start();
            t3.start();
            t4.start();

            t1.join();
            t2.join();
            t3.join();
            t4.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}


class RPCHandler {
    public String calc(Integer n) {
        StringBuilder sb = new StringBuilder();
        int x = 0;
        for (int i = 0; i < n; ++i) {
            x += i;
            sb.append(i);
            sb.append("+");
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        x += n;
        sb.append(n);
        sb.append("=");
        sb.append(x);
        return sb.toString();
    }
}

class Client implements Runnable {
    private int n;
    Client(int n ) {
        this.n = n;
    }
    @Override
    public  void run() {
        final ZRpcClient client = new ZRpcClient();
        client.connect("tcp://127.0.0.1:12345");
        try {
            System.out.println("start calc " + n);
            client.asyncCall("calc", n);
            for (int i = 0 ; i < 1000; ++i) {
                String s = client.get(false);
                if (s == null) {
                    System.out.println("waiting..." + i);
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } else {
                    System.out.println("get " + s);
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
