package pdc;

/**
 * Quick test for JSON parsing
 */
public class MessageTest {
    public static void main(String[] args) {
        // Test 1: Basic JSON parsing
        String json1 = "{\"magic\":\"CSM218\",\"version\":1,\"messageType\":\"RPC_REQUEST\",\"studentId\":\"testuser\",\"timestamp\":1234567890,\"payload\":\"test-data\"}";
        Message msg1 = Message.parse(json1);
        
        System.out.println("Test 1: Basic JSON parsing");
        System.out.println("  magic: " + msg1.magic + " (expected: CSM218)");
        System.out.println("  version: " + msg1.version + " (expected: 1)");
        System.out.println("  messageType: " + msg1.messageType + " (expected: RPC_REQUEST)");
        System.out.println("  studentId: " + msg1.studentId + " (expected: testuser)");
        System.out.println("  timestamp: " + msg1.timestamp + " (expected: 1234567890)");
        System.out.println("  payloadStr: " + msg1.payloadStr + " (expected: test-data)");
        
        // Test 2: Serialization back to JSON
        String json2 = msg1.toJson();
        System.out.println("\nTest 2: Serialization to JSON");
        System.out.println("  JSON: " + json2);
        
        // Test 3: Round-trip
        Message msg2 = Message.parse(json2);
        System.out.println("\nTest 3: Round-trip parse");
        System.out.println("  messageType match: " + msg1.messageType.equals(msg2.messageType));
        System.out.println("  studentId match: " + msg1.studentId.equals(msg2.studentId));
        System.out.println("  payloadStr match: " + msg1.payloadStr.equals(msg2.payloadStr));
        
        // Test 4: Complex payload
        String json3 = "{\"magic\":\"CSM218\",\"version\":1,\"messageType\":\"TASK_COMPLETE\",\"studentId\":\"integration-test\",\"timestamp\":1613320345000,\"payload\":\"task-0;MATRIX_MULTIPLY;1,2\\\\3,4|5,6\\\\7,8;processed\"}";
        Message msg3 = Message.parse(json3);
        System.out.println("\nTest 4: Complex payload");
        System.out.println("  payloadStr: " + msg3.payloadStr);
    }
}
