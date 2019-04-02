import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.*;
import java.util.Properties;

public class Client {
    enum Option {
        SET, GET, DESC, EXIT
    }

    public static void main(String[] args) {
        try {
            // Get Configs
            Properties prop = new Properties();
            InputStream is = new FileInputStream("chordht.cfg");
            prop.load(is);
            String[] nodeInfo = getNode(prop);

            if (args.length == 1) {
                // set all the entries in the file
                bulkSet(nodeInfo, args[0]);
            }

            Option option = Option.SET;
            Console console = System.console();

            while (option != Option.EXIT) {
                // UI Loop
                option = Option.valueOf(console.readLine("CHOOSE>\n0: GET, 1: SET, 2: DESC, 3: EXIT\n> "));
                switch (option) {
                    case GET:
                        String key = console.readLine("Enter Key: ");
                        nodeInfo = getNode(prop);
                        if(nodeInfo[0].equals("NACK")) {
                            console.printf("DHT isn't ready yet, try again later.\n");
                        }
                        else {
                            get(nodeInfo, key);
                        }
                        break;
                    case SET:
                        String[] keyValue = console.readLine("Enter Key, Value: ").split("\\s*,\\s*");
                        nodeInfo = getNode(prop);
                        if(nodeInfo[0].equals("NACK")) {
                            console.printf("DHT isn't ready yet, try again later.\n");
                        }
                        else {
                            set(nodeInfo, keyValue[0], keyValue[1]);
                        }
                        break;
                    case DESC:
                        nodeInfo = getNode(prop);
                        if(nodeInfo[0].equals("NACK")) {
                            console.printf("DHT isn't ready yet, try again later.\n");
                        }
                        else {
                            desc(nodeInfo);
                        }
                        break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String[] getNode(Properties prop) throws TException {
        // create client connection
        String superNodeAddress = prop.getProperty("supernode.address");
        Integer superNodePort = Integer.valueOf(prop.getProperty("supernode.port"));
        TTransport transport = new TSocket(superNodeAddress, superNodePort);
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        SuperNodeService.Client client = new SuperNodeService.Client(protocol);
        return client.GetNode().split("\\s*,\\s*");
    }

    // helper methods

    private static void bulkSet(String[] nodeInfo, String fileName) throws FileNotFoundException {
        BufferedReader br = new BufferedReader(new FileReader(fileName));

    }

    private static void set(String[] nodeInfo, String key, String value) {

    }

    private static void get(String[] nodeInfo, String key) throws TException {
        // create client connection
        TTransport transport = new TSocket(nodeInfo[0], Integer.valueOf(nodeInfo[1]));
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        NodeService.Client client = new NodeService.Client(protocol);
        GetResponse getResponse = client.get_(key);
    }

    private static void desc(String[] nodeInfo) {

    }

    private static void ping(String[] nodeInfo) {

    }
}
