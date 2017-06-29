package org.zerorpc;

import com.google.common.primitives.Bytes;
import org.msgpack.core.*;
import org.msgpack.value.*;

import java.io.*;
import java.rmi.RemoteException;
import java.util.*;

class Event {
    private String response_to;
    private String message_id;
    private int v;
    private String method;
    private Value[] args;

    private Event(String message_id, String response_to, String meth, Value... args) {
        this.message_id = message_id != null ? message_id : UUID.randomUUID().toString().replaceAll("-", "");
        this.response_to = response_to;
        this.v = 3;
        this.method = meth;
        this.args = args;
    }

    String getMethod() {
        return this.method;
    }

    Value[] getArgs() {
        return this.args;
    }

    static Event newRequest(String meth, Object... args) throws IOException {
        Value[] vargs = new Value[args.length];
        for (int i = 0; i < args.length; ++i) {
            vargs[i] = Convert.toValue(args[i]);
        }
        return new Event(null, null, meth, vargs);
    }

    static Event deserialize(byte[] bytes) throws IOException {
        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(bytes);
        Value v = unpacker.unpackValue();
        ArrayValue event = v.asArrayValue();
        Map<Value, Value> header = event.get(0).asMapValue().map();

        String message_id = null;
        String response_to = null;
        String method = event.get(1).asRawValue().asString();
        if (method.equals("_zpc_hb")) {
            message_id = header.get(ValueFactory.newString("message_id")).asRawValue().asString();
            response_to = header.get(ValueFactory.newString("response_to")).asRawValue().asString();
        } else {
            response_to = header.get(ValueFactory.newString("message_id")).asRawValue().asString();
        }

        List<Value> argsarray = event.get(2).asArrayValue().list();
        Value[] args = new Value[argsarray.size()];
        args = argsarray.toArray(args);
        return new Event(message_id, response_to, method, args);
    }

    /** for call response * */
    public void serialize(ByteArrayOutputStream bytes, Object obj) throws IOException {
        Map<Value, Value> hdr = new HashMap<>();
        hdr.put(ValueFactory.newString("v"), ValueFactory.newInteger(this.v));
        hdr.put(ValueFactory.newString("message_id"), ValueFactory.newString(this.message_id));
        hdr.put(ValueFactory.newString("response_to"), ValueFactory.newString(this.response_to));
        Value h = ValueFactory.newMap(hdr);

        Value m;
        Value[] vv;
        /** for heartbeat * */
        if (this.method.equals("_zpc_hb")) {
            m = ValueFactory.newString("_zpc_hb");
            vv = this.args;
        } else {
            m = ValueFactory.newString("OK");
            Value v = Convert.toValue(obj);
            vv = new Value[]{v};
        }

        Value[] array = new Value[]{h, m, ValueFactory.newArray(vv)};
        final MessagePacker packer =  MessagePack.newDefaultPacker(bytes);
        packer.packValue(ValueFactory.newArray(array));
        packer.close();
    }

    /** for exception * */
    public void serialize(ByteArrayOutputStream bytes, Exception e) throws IOException {
        Map<Value, Value> hdr = new HashMap<>();
        hdr.put(ValueFactory.newString("v"), ValueFactory.newInteger(this.v));
        hdr.put(ValueFactory.newString("message_id"), ValueFactory.newString(this.message_id));
        hdr.put(ValueFactory.newString("response_to"), ValueFactory.newString(this.response_to));
        Value h = ValueFactory.newMap(hdr);

        Value e1 = ValueFactory.newString(e.getClass().getName());
        Value e2 = ValueFactory.newString(e.getMessage() != null ? e.getMessage(): "");
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        Value e3 = ValueFactory.newString(sw.toString());
        Value[] vv = new Value[]{e1, e2, e3};

        Value[] array = new Value[]{h, ValueFactory.newString("ERR"), ValueFactory.newArray(vv)};
        final MessagePacker packer =  MessagePack.newDefaultPacker(bytes);
        packer.packValue(ValueFactory.newArray(array));
        packer.close();
    }

    /** for call request * */
    public void serialize(ByteArrayOutputStream bytes) throws IOException {
        Map<Value, Value> hdr = new HashMap<>();
        hdr.put(ValueFactory.newString("v"), ValueFactory.newInteger(this.v));
        hdr.put(ValueFactory.newString("message_id"), ValueFactory.newString(this.message_id));
        Value h = ValueFactory.newMap(hdr);

        Value m = ValueFactory.newString(this.method);

        Value[] array = new Value[]{h, m, ValueFactory.newArray(this.args)};
        final MessagePacker packer =  MessagePack.newDefaultPacker(bytes);
        packer.packValue(ValueFactory.newArray(array));
        packer.close();
    }

    Object getResult() throws IOException {
        if (this.method.equals("ERR")) {
            String e = this.args[0].toString();
            String e1 = this.args[1].toString();
            String e2 = this.args[2].toString();
            throw new RemoteException(e + e1, new Throwable(e2));
        }
        Value v = this.args[0];
        return Convert.toObject(v);
    }
}

class Convert {
    static Object toObject(Value v) {
        if (v.getValueType() == ValueType.NIL) {
            return null;
        } else if (v.getValueType() == ValueType.BOOLEAN) {
            return v.asBooleanValue().getBoolean();
        } else if (v.getValueType() == ValueType.INTEGER) {
            return v.asIntegerValue().toInt();
        } else if (v.getValueType() == ValueType.FLOAT) {
            return v.asFloatValue().toDouble();
        } else if (v.getValueType() == ValueType.BINARY) {
            return v.asBinaryValue().asByteArray();
        } else if (v.getValueType() == ValueType.STRING) {
            return v.asStringValue().asString();
        } else if (v.getValueType() == ValueType.ARRAY) {
            ArrayList<Object> array = new ArrayList<>();
            for (Value x: v.asArrayValue().list()) {
                array.add(toObject(x));
            }
            return array;
        } else if (v.getValueType() == ValueType.MAP) {
            Map<Object, Object> map = new HashMap<>();
            for (Map.Entry<Value, Value> x: v.asMapValue().entrySet()) {
                map.put(toObject(x.getKey()), toObject(x.getValue()));
            }
            return map;
        } else {
            return null;
        }
    }

    static Value toValue(Object o) {
        if (o == null) {
            return ValueFactory.newNil();
        } else if (o instanceof Boolean) {
            return ValueFactory.newBoolean((boolean)o);
        } else if (o instanceof Integer) {
            return ValueFactory.newInteger((int) o);
        } else if (o instanceof Long) {
            return ValueFactory.newInteger((long) o);
        } else if (o instanceof Float) {
            return ValueFactory.newFloat((float)o);
        } else if (o instanceof Double) {
            return ValueFactory.newFloat((double)o);
        } else if (o instanceof Bytes) {
            return ValueFactory.newBinary((byte[]) o);
        } else if (o instanceof String) {
            return ValueFactory.newString((String) o);
        } else if (o instanceof ArrayList) {
            ArrayList<Object> arr = (ArrayList<Object>)o;
            Value[] array = new Value[arr.size()];
            for (int i = 0; i < arr.size(); ++i) {
                array[i] = toValue(arr.get(i));
            }
            return ValueFactory.newArray(array);
        } else if (o instanceof Map) {
            Map<Object, Object> mm =(Map<Object, Object>)o;
            Map<Value, Value> map = new HashMap<>();
            for (Map.Entry<Object, Object> x: mm.entrySet()) {
                map.put(toValue(x.getKey()), toValue(x.getValue()));
            }
            return ValueFactory.newMap(map);
        } else {
            return ValueFactory.newNil();
        }
    }
}