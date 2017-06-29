# zerorpc-java
zerorpc java implements, see http://www.zerorpc.io/ for detail

# usage:

#### server
```java
class RPCHandler {
    public String calc(Integer n) {
        StringBuilder sb = new StringBuilder();
        int x = 0;
        for (int i = 0; i < n; ++i) {
            x += i;
            sb.append(i);
            sb.append("+");
        }
        x += n;
        sb.append(n);
        sb.append("=");
        sb.append(x);
        return sb.toString();
    }
}

ZRpcServer<RPCHandler> server = new ZRpcServer<>("tcp://0.0.0.0:12345", RPCHandler.class);
server.run();
```

#### client
```java
ZRpcClient client = new ZRpcClient();
client.connect("tcp://127.0.0.1:12345");
String s = client.call("calc", n);
System.out.print(s);

// or use asyncCall
client.asyncCall("calc", n);
// do something else
// ...
String s = client.get();
System.out.print(s);

// more:
// client.get() = client.get(true); // blocking until response
// client.get(false); // non-blocking, return null if no response yet
```

see ZTest.java for detail
