package pdc;

import java.io.*;

public class Message {
    public String magic;
    public int version;
    public String type;              // messageType alias
    public String messageType;       // CSM218 schema field
    public String sender;
    public String studentId;         // CSM218 schema field
    public long timestamp;
    public byte[] payload;

    public Message() {}

    // Convert message to byte array
    public byte[] pack() {
        try {
            // Sync alias fields
            if (messageType == null) messageType = type;
            if (studentId == null) studentId = sender;
            
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
            
            // Sync alias fields
            msg.messageType = msg.type;
            msg.studentId = msg.sender;

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

    // Validate message protocol
    public void validate() throws Exception {
        if (magic == null || !magic.equals("CSM218")) {
            throw new Exception("Missing or invalid magic field (should be CSM218)");
        }
        if (version != 1) {
            throw new Exception("Invalid version (should be 1)");
        }
        if (type == null || type.isEmpty()) {
            throw new Exception("Missing message type");
        }
        if (sender == null || sender.isEmpty()) {
            throw new Exception("Missing sender");
        }
        if (timestamp <= 0) {
            throw new Exception("Invalid timestamp");
        }
    }
}
