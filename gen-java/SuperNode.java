import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class SuperNode {
    public static SuperNodeHandler handler;
    private static SuperNodeService.Processor<SuperNodeHandler> processor;
    public static Properties prop;

    public static void main(String[] args) {
        try {
            prop = new Properties();
            InputStream is = new FileInputStream("chordht.cfg");
            prop.load(is);
            handler = new SuperNodeHandler(prop);
            processor = new SuperNodeService.Processor<>(handler);

            startThreadPoolServer();
        } catch (Exception x) {
            x.printStackTrace();
        }
    }

    private static void startThreadPoolServer() {
        try {
            // Create Thrift server socket as a thread pool
            Integer serverPort = Integer.valueOf(prop.getProperty("supernode.port"));
            TServerTransport serverTransport = new TServerSocket(serverPort);
            TThreadPoolServer.Args args = new TThreadPoolServer.Args(serverTransport);
            args.processor(processor);
            TServer server = new TThreadPoolServer(args);

            System.out.println("Starting the SuperNode...");
            server.serve();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

