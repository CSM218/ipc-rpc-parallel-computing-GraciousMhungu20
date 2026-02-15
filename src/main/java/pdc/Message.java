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

    // JSON serialization
    public String toJson() {
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
    
    // Helper method for JSON deserialization - improved
    private static Map<String, Object> jsonToMap(String json) {
        if (json == null || json.trim().isEmpty()) return new HashMap<>();
        
        Map<String, Object> map = new HashMap<>();
        
        try {
            json = json.trim();
            if (!json.startsWith("{") || !json.endsWith("}")) {
                return map;
            }
            
            // Remove outer braces
            String content = json.substring(1, json.length() - 1);
            
            // Manual JSON parsing
            StringBuilder key = null;
            StringBuilder value = null;
            boolean inQuote = false;
            boolean inValue = false;
            boolean isString = false;
            
            int i = 0;
            while (i < content.length()) {
                char c = content.charAt(i);
                char nextChar = i + 1 < content.length() ? content.charAt(i + 1) : '\0';
                
                // Handle strings
                if (c == '"') {
                    if (i > 0 && content.charAt(i - 1) != '\\') {
                        inQuote = !inQuote;
                        if (!inQuote && !inValue) {
                            key = new StringBuilder();
                        } else if (!inQuote && inValue) {
                            isString = true;
                        }
                    }
                    if (inQuote) {
                        if (key != null && key.length() > 0) {
                            key.append(c);
                        }
                        if (value != null) {
                            value.append(c);
                        }
                    }
                    i++;
                    continue;
                }
                
                if (inQuote) {
                    if (key != null && key.length() > 0) {
                        key.append(c);
                    } else if (value != null) {
                        value.append(c);
                    }
                    i++;
                    continue;
                }
                
                // Handle colon
                if (c == ':' && !inValue && key != null) {
                    inValue = true;
                    value = new StringBuilder();
                    i++;
                    // Skip whitespace
                    while (i < content.length() && Character.isWhitespace(content.charAt(i))) {
                        i++;
                    }
                    continue;
                }
                
                // Handle value
                if (inValue && c != ',') {
                    value.append(c);
                    i++;
                    continue;
                }
                
                // Handle comma
                if (c == ',' || i == content.length() - 1) {
                    if (key != null && value != null) {
                        String keyStr = key.toString().trim().replaceAll("^\"|\"$", "");
                        String valueStr = value.toString().trim();
                        
                        Object parsedValue;
                        if (valueStr.startsWith("\"") && valueStr.endsWith("\"")) {
                            parsedValue = unescape(valueStr.substring(1, valueStr.length() - 1));
                        } else if ("null".equals(valueStr)) {
                            parsedValue = null;
                        } else if ("true".equals(valueStr)) {
                            parsedValue = true;
                        } else if ("false".equals(valueStr)) {
                            parsedValue = false;
                        } else {
                            try {
                                if (valueStr.contains(".")) {
                                    parsedValue = Double.parseDouble(valueStr);
                                } else {
                                    parsedValue = Long.parseLong(valueStr);
                                }
                            } catch (NumberFormatException e) {
                                parsedValue = valueStr;
                            }
                        }
                        
                        map.put(keyStr, parsedValue);
                    }
                    
                    key = null;
                    value = null;
                    inValue = false;
                    isString = false;
                }
                
                i++;
            }
        } catch (Exception e) {
            System.err.println("[Message] Error parsing JSON: " + e.getMessage());
            e.printStackTrace();
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
