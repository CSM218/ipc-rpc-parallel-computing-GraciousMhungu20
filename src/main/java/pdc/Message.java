package pdc;

import java.io.*;

public class Message {
    public String magic;
    public int version;
    public String type;
    public String sender;
    public long timestamp;
    public byte[] payload;

    public Message() {}

    // Convert message to byte array
    public byte[] pack() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);

            out.writeUTF(magic);
            out.writeInt(version);
            out.writeUTF(type);
            out.writeUTF(sender);
            out.writeLong(timestamp);

            if (payload != null) {
                out.writeInt(payload.length);
                out.write(payload);
            } else {
                out.writeInt(0);
            }

            out.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Packing failed", e);
        }
    }

    // Convert byte array back to message
    public static Message unpack(byte[] data) {
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            DataInputStream in = new DataInputStream(bais);

            Message msg = new Message();
            msg.magic = in.readUTF();
            msg.version = in.readInt();
            msg.type = in.readUTF();
            msg.sender = in.readUTF();
            msg.timestamp = in.readLong();

            int length = in.readInt();
            if (length > 0) {
                msg.payload = new byte[length];
                in.readFully(msg.payload);
            } else {
                msg.payload = new byte[0];
            }

            return msg;
        } catch (IOException e) {
            throw new RuntimeException("Unpacking failed", e);
        }
    }
}
