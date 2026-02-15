package pdc;

import java.io.*;
import java.util.*;

public class Message {
    public String magic;
    public int version;
    public String type;              // messageType alias
    public String messageType;       // CSM218 schema field
    public String sender;
    public String studentId;         // CSM218 schema field
    public long timestamp;
    public byte[] payload;
    public String payloadStr;        // String version for JSON

    public Message() {
        this.magic = "CSM218";
        this.version = 1;
        this.timestamp = System.currentTimeMillis();
    }
    
    // Ensure alias fields are in sync
    public void syncAliases() {
        if (this.type != null && this.messageType == null) this.messageType = this.type;
        if (this.messageType != null && this.type == null) this.type = this.messageType;
        if (this.sender != null && this.studentId == null) this.studentId = this.sender;
        if (this.studentId != null && this.sender == null) this.sender = this.studentId;
    }

    // JSON serialization
    public String toJson() {
        syncAliases();
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("magic", magic != null ? magic : "CSM218");
        map.put("version", version > 0 ? version : 1);
        map.put("messageType", type != null ? type : messageType);
        map.put("studentId", sender != null ? sender : studentId);
        map.put("timestamp", timestamp > 0 ? timestamp : System.currentTimeMillis());
        map.put("payload", payloadStr != null ? payloadStr : (payload != null ? new String(payload) : ""));
        
        return mapToJson(map);
    }
    
    // JSON deserialization
    public static Message parse(String json) {
        Message msg = new Message();
        Map<String, Object> map = jsonToMap(json);
        
        if (map == null) return msg;
        
        msg.magic = (String) map.getOrDefault("magic", "CSM218");
        msg.version = ((Number) map.getOrDefault("version", 1)).intValue();
        msg.messageType = (String) map.getOrDefault("messageType", "");
        msg.type = msg.messageType;
        msg.studentId = (String) map.getOrDefault("studentId", "");
        msg.sender = msg.studentId;
        msg.timestamp = ((Number) map.getOrDefault("timestamp", System.currentTimeMillis())).longValue();
        
        Object payloadObj = map.get("payload");
        if (payloadObj instanceof String) {
            msg.payloadStr = (String) payloadObj;
            msg.payload = msg.payloadStr.getBytes();
        }
        
        msg.syncAliases();
        return msg;
    }

    // Convert message to byte array (binary serialization)
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

    // Convert byte array back to message (binary deserialization)
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
        if (type == null && messageType == null) {
            throw new Exception("Missing message type");
        }
        if (sender == null && studentId == null) {
            throw new Exception("Missing sender/studentId");
        }
        if (timestamp <= 0) {
            throw new Exception("Invalid timestamp");
        }
    }
    
    // Helper method for JSON serialization
    private String mapToJson(Map<String, Object> map) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        boolean first = true;
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (!first) sb.append(",");
            first = false;
            sb.append("\"").append(entry.getKey()).append("\":");
            Object value = entry.getValue();
            if (value instanceof String) {
                sb.append("\"").append(escape((String) value)).append("\"");
            } else if (value instanceof Number) {
                sb.append(value);
            } else {
                sb.append("null");
            }
        }
        sb.append("}");
        return sb.toString();
    }
    
    // Helper method for JSON deserialization - ultra-simple and robust
    private static Map<String, Object> jsonToMap(String json) {
        if (json == null || json.trim().isEmpty()) return new HashMap<>();
        
        Map<String, Object> map = new HashMap<>();
        
        try {
            json = json.trim();
            if (!json.startsWith("{") || !json.endsWith("}")) {
                return map;
            }
            
            // Extract each field using simple substring parsing
            // Expected format: {"magic":"CSM218","version":1,"messageType":"...","studentId":"...","timestamp":12345,"payload":"..."}
            
            // Parse magic
            int idx = json.indexOf("\"magic\":");
            if (idx >= 0) {
                idx += 8; // length of "\"magic\":"
                int endIdx = json.indexOf("\"", idx + 1); // Find closing quote
                if (endIdx > idx) {
                    String value = json.substring(idx + 1, endIdx);
                    map.put("magic", unescape(value));
                }
            }
            
            // Parse version
            idx = json.indexOf("\"version\":");
            if (idx >= 0) {
                idx += 10;
                int commaIdx = json.indexOf(",", idx);
                int braceIdx = json.indexOf("}", idx);
                int endIdx = commaIdx >= 0 && commaIdx < braceIdx ? commaIdx : braceIdx;
                String valueStr = json.substring(idx, endIdx).trim();
                try {
                    map.put("version", Long.parseLong(valueStr));
                } catch (NumberFormatException e) {
                    map.put("version", 1);
                }
            }
            
            // Parse messageType
            idx = json.indexOf("\"messageType\":");
            if (idx >= 0) {
                idx += 15; // length of "\"messageType\":"
                int endIdx = json.indexOf("\"", idx + 1);
                if (endIdx > idx) {
                    String value = json.substring(idx + 1, endIdx);
                    map.put("messageType", unescape(value));
                }
            }
            
            // Parse studentId
            idx = json.indexOf("\"studentId\":");
            if (idx >= 0) {
                idx += 13; // length of "\"studentId\":"
                int endIdx = json.indexOf("\"", idx + 1);
                if (endIdx > idx) {
                    String value = json.substring(idx + 1, endIdx);
                    map.put("studentId", unescape(value));
                }
            }
            
            // Parse timestamp
            idx = json.indexOf("\"timestamp\":");
            if (idx >= 0) {
                idx += 12;
                int commaIdx = json.indexOf(",", idx);
                int braceIdx = json.indexOf("}", idx);
                int endIdx = commaIdx >= 0 && commaIdx < braceIdx ? commaIdx : braceIdx;
                String valueStr = json.substring(idx, endIdx).trim();
                try {
                    map.put("timestamp", Long.parseLong(valueStr));
                } catch (NumberFormatException e) {
                    map.put("timestamp", System.currentTimeMillis());
                }
            }
            
            // Parse payload - this is tricky because it can contain "," and other special chars
            idx = json.indexOf("\"payload\":");
            if (idx >= 0) {
                idx += 11; // length of "\"payload\":"
                if (idx < json.length() && json.charAt(idx) == '"') {
                    idx++; // skip opening quote
                    StringBuilder payload = new StringBuilder();
                    boolean escaped = false;
                    while (idx < json.length()) {
                        char c = json.charAt(idx);
                        if (escaped) {
                            if (c == '"') payload.append('"');
                            else if (c == '\\') payload.append('\\');
                            else if (c == 'n') payload.append('\n');
                            else if (c == 'r') payload.append('\r');
                            else payload.append(c);
                            escaped = false;
                        } else if (c == '\\') {
                            escaped = true;
                        } else if (c == '"') {
                            // Found closing quote
                            map.put("payload", payload.toString());
                            break;
                        } else {
                            payload.append(c);
                        }
                        idx++;
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("[Message] Error parsing JSON: " + e.getMessage());
        }
        
        return map;
    }
    
    private static String escape(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r");
    }
    
    private static String unescape(String s) {
        if (s == null) return "";
        return s.replace("\\\"", "\"").replace("\\n", "\n").replace("\\r", "\r").replace("\\\\", "\\");
    }
}
