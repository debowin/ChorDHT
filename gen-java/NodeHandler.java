import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class NodeHandler implements NodeService.Iface {
    class FingerTableEntry {
        String nodeInfo;
        // interval
        Integer start;
        Integer end; //exclusive
    }

    private Properties prop;
    private String address;
    private Integer port;
    private Integer id;
    private Integer mbits;
    private Integer keySpace;
    private ArrayList<FingerTableEntry> fingerTable;
    private HashMap<String, String> bookTitleGenreMapping;
    private String successor;
    private String predecessor;

    NodeHandler(Properties properties, Integer nodeNumber) {
        prop = properties;
        address = prop.getProperty("node.addresses").split("\\s*,\\s*")[nodeNumber];
        port = Integer.valueOf(prop.getProperty("node.ports").split("\\s*,\\s*")[nodeNumber]);
        mbits = Integer.valueOf(prop.getProperty("nodes.mbits"));
        keySpace = (int) Math.pow(2, mbits);
        bookTitleGenreMapping = new HashMap<>();

        // Ask to join the DHT
        joinDHT();
    }

    private void joinDHT() {
        try {
            JoinResponse joinResponse;
            TTransport sntransport;
            TProtocol protocol;
            SuperNodeService.Client snclient;
            try {
                String superNodeAddress = prop.getProperty("supernode.address");
                Integer superNodePort = Integer.valueOf(prop.getProperty("supernode.port"));
                sntransport = new TSocket(superNodeAddress, superNodePort);

                sntransport.open();
                protocol = new TBinaryProtocol(sntransport);
                snclient = new SuperNodeService.Client(protocol);
                Long waitDelay = Long.parseLong(prop.getProperty("node.wait"));
                do {
                    // repeat until join request is granted
                    joinResponse = snclient.Join(address, port);
                    Thread.sleep(waitDelay);
                } while (joinResponse.id == -1);
            } catch (TException e){
                System.out.println("Error connecting to the SuperNode!\n");
                System.exit(-1);
                return;
            }
            String[] neighborNodeInfo = joinResponse.nodeInfo.split("\\s*,\\s*");
            String neighborNodeAddress = neighborNodeInfo[0];
            Integer neighborNodePort = Integer.valueOf(neighborNodeInfo[1]);
            id = joinResponse.id;
            // build finger table
            fingerTable = new ArrayList<>();
            for (int i = 0; i < mbits; i++) {
                FingerTableEntry ftEntry = new FingerTableEntry();
                ftEntry.start = (id + (int) Math.pow(2, i)) % keySpace;
                ftEntry.end = (id + (int) Math.pow(2, i + 1)) % keySpace;
                fingerTable.add(ftEntry);
            }
            if (neighborNodeAddress.equals(address) && neighborNodePort.equals(port)) {
                // if first node to join the DHT
                predecessor = joinResponse.nodeInfo;
                // fill up the finger table with the same node
                for (int i = 0; i < mbits; i++) {
                    fingerTable.get(i).nodeInfo = joinResponse.nodeInfo;
                }
                successor = fingerTable.get(0).nodeInfo;
            } else {

                initFingerTable(neighborNodeInfo);

                // update successor's predecessor
                String[] successorNodeInfo = successor.split("\\s*,\\s*");
                TTransport ntransport = new TSocket(successorNodeInfo[0], Integer.valueOf(successorNodeInfo[1]));
                ntransport.open();
                protocol = new TBinaryProtocol(ntransport);
                NodeService.Client nclient = new NodeService.Client(protocol);
                nclient.updatePredecessor(String.join(",", address, String.valueOf(port), String.valueOf(id)));
                ntransport.close();

                // update other nodes' finger tables
                for (int i = 0; i < mbits; i++) {
                    // find the last node pred whose ith finger might be current node
                    String[] pred = findPredecessor((id - (int) Math.pow(2, i) + keySpace + 1) % keySpace).split("\\s*,\\s*");
                    if (!pred[0].equals(address) || !pred[1].equals(String.valueOf(port))) {
                        // no need to update own finger table
                        ntransport = new TSocket(pred[0], Integer.valueOf(pred[1]));
                        ntransport.open();
                        protocol = new TBinaryProtocol(ntransport);
                        nclient = new NodeService.Client(protocol);
                        nclient.updateFingerTable(String.join(",", address, String.valueOf(port), String.valueOf(id)), i);
                        ntransport.close();
                    }
                }
            }
            // now that join is complete
            System.out.println("DHT Join Completed Successfully!");
            snclient.PostJoin(address, port);
            sntransport.close();
            System.out.print(getNodeDetails());
        } catch (TException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private String getNodeDetails() {
        StringBuilder output = new StringBuilder(String.format("<===*******[NODE %d Details]*******===>\n" +
                        "KeySpace (%d, %d] - %d Entries\n\t\tPredecessor\t\t\t\tSuccessor\n" +
                        "%s\t%s\n" +
                        "Finger Table\nStart\tEnd\tNode\n", id, Integer.valueOf(predecessor.split("\\s*,\\s*")[2]), id,
                bookTitleGenreMapping.size(), predecessor, successor));
        for (int i = 0; i < mbits; i++) {
            output.append(String.format("%d\t%d\t%s\n", fingerTable.get(i).start,
                    fingerTable.get(i).end, fingerTable.get(i).nodeInfo));
        }
        if(!bookTitleGenreMapping.isEmpty()) {
            // also include entry table if not empty
            output.append(String.format("Entry Table\n%30s\t%15s\n", "Book Title", "Genre"));
            for (Map.Entry<String, String> kv : bookTitleGenreMapping.entrySet()) {
                output.append(String.format("%30s\t%15s\n", kv.getKey(), kv.getValue()));
            }
        }
        return output.toString();
    }

    private void initFingerTable(String[] neighborNodeInfo) throws TException {
        System.out.printf("initFingerTable(%s,%s,%s)\n", neighborNodeInfo[0], neighborNodeInfo[1], neighborNodeInfo[2]);

        // get the successor node from neighbor
        TTransport transport = new TSocket(neighborNodeInfo[0], Integer.valueOf(neighborNodeInfo[1]));
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        NodeService.Client client = new NodeService.Client(protocol);
        fingerTable.get(0).nodeInfo = client.findSuccessor(fingerTable.get(0).start);
        successor = fingerTable.get(0).nodeInfo;
        transport.close();

        // get predecessor node from the successor node
        String[] successorNodeInfo = successor.split("\\s*,\\s*");
        transport = new TSocket(successorNodeInfo[0], Integer.valueOf(successorNodeInfo[1]));
        transport.open();
        protocol = new TBinaryProtocol(transport);
        client = new NodeService.Client(protocol);
        predecessor = client.getPredecessor();
        transport.close();

        // fill up the rest of the fingers
        for (int i = 0; i < mbits - 1; i++) {
            // [)
            if (belongsToRange(fingerTable.get(i + 1).start, (id - 1 + keySpace) % keySpace,
                    Integer.valueOf(fingerTable.get(i).nodeInfo.split("\\s*,\\s*")[2]))) {
                fingerTable.get(i + 1).nodeInfo = fingerTable.get(i).nodeInfo;
            } else {
                transport = new TSocket(neighborNodeInfo[0], Integer.valueOf(neighborNodeInfo[1]));
                transport.open();
                protocol = new TBinaryProtocol(transport);
                client = new NodeService.Client(protocol);
                fingerTable.get(i + 1).nodeInfo = client.findSuccessor(fingerTable.get(i + 1).start);
                transport.close();
            }
        }
        System.out.printf("initFingerTable(%s,%s,%s) returns\n", neighborNodeInfo[0], neighborNodeInfo[1], neighborNodeInfo[2]);
    }

    @Override
    public boolean ping() throws TException {
        System.out.println("PING!");
        return true;
    }

    @Override
    public String findSuccessor(int query_id) throws TException {
        System.out.printf("findSuccessor(%d)\n", query_id);
        String[] nodeInfo = findPredecessor(query_id).split("\\s*,\\s*");
        if (nodeInfo[0].equals(address) && nodeInfo[1].equals(String.valueOf(port))) {
            // local procedure call
            System.out.printf("findSuccessor(%d) returns %s\n", query_id, getSuccessor());
            return getSuccessor();
        }
        // remote procedure call
        TTransport transport = new TSocket(nodeInfo[0], Integer.valueOf(nodeInfo[1]));
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        NodeService.Client client = new NodeService.Client(protocol);
        String successor_ = client.getSuccessor();
        System.out.printf("findSuccessor(%d) returns %s\n", query_id, successor_);
        transport.close();
        return successor_;
    }

    @Override
    public String findPredecessor(int query_id) throws TException {
        System.out.printf("findPredecessor(%d)\n", query_id);
        String[] nodeInfo_ = {address, String.valueOf(port), String.valueOf(id)};
        Integer successorID = Integer.valueOf(getSuccessor().split("\\s*,\\s*")[2]);
        Boolean doesntBelongToRange = !belongsToRange(query_id, Integer.valueOf(nodeInfo_[2]),
                (successorID + 1) % keySpace); // (]
        while (doesntBelongToRange) {
            // update nodeInfo_
            if (nodeInfo_[0].equals(address) && nodeInfo_[1].equals(String.valueOf(port))) {
                // local procedure call
                nodeInfo_ = findClosestPrecedingFinger(query_id).split("\\s*,\\s*");
            } else {
                // remote procedure call
                TTransport transport = new TSocket(nodeInfo_[0], Integer.valueOf(nodeInfo_[1]));
                transport.open();
                TProtocol protocol = new TBinaryProtocol(transport);
                NodeService.Client client = new NodeService.Client(protocol);
                nodeInfo_ = client.findClosestPrecedingFinger(query_id).split("\\s*,\\s*");
                transport.close();
            }
            // update doesntBelongToRange
            if (nodeInfo_[0].equals(address) && nodeInfo_[1].equals(String.valueOf(port))) {
                // local procedure call
                successorID = Integer.valueOf(getSuccessor().split("\\s*,\\s*")[2]);
                doesntBelongToRange = !belongsToRange(query_id, Integer.valueOf(nodeInfo_[2]),
                        (successorID + 1) % keySpace); // (]
            } else {
                // remote procedure call
                TTransport transport = new TSocket(nodeInfo_[0], Integer.valueOf(nodeInfo_[1]));
                transport.open();
                TProtocol protocol = new TBinaryProtocol(transport);
                NodeService.Client client = new NodeService.Client(protocol);
                successorID = Integer.valueOf(client.getSuccessor().split("\\s*,\\s*")[2]);
                doesntBelongToRange = !belongsToRange(query_id, Integer.valueOf(nodeInfo_[2]),
                        (successorID + 1) % keySpace); // (]
                transport.close();
            }
        }
        System.out.printf("findPredecessor(%d) returns %s\n", query_id, String.join(",", nodeInfo_));
        return String.join(",", nodeInfo_);
    }

    @Override
    public String findClosestPrecedingFinger(int query_id) throws TException {
        System.out.printf("findClosestPrecedingFinger(%d)\n", query_id);
        for (int i = mbits - 1; i > -1; i--) {
            // ()
            if (belongsToRange(Integer.valueOf(fingerTable.get(i).nodeInfo.split("\\s*,\\s*")[2]),
                    id, query_id)) {
                System.out.printf("findClosestPrecedingFinger(%d) returns %s\n", query_id,
                        fingerTable.get(i).nodeInfo);
                return fingerTable.get(i).nodeInfo;
            }
        }
        System.out.printf("findClosestPrecedingFinger(%d) returns %s\n", query_id, predecessor);
        return predecessor;
    }

    @Override
    public String getSuccessor() throws TException {
        return successor;
    }

    @Override
    public String getPredecessor() throws TException {
        return predecessor;
    }

    /**
     * Update own Finger Table at position 'i_' with potential 'successor' node if needed.
     *
     * @param successor_
     * @param i_
     * @throws TException
     */
    @Override
    public void updateFingerTable(String successor_, int i_) throws TException {
        System.out.printf("updateFingerTable(%s, %d)\n", successor_, i_);
        Integer successorID = Integer.valueOf(successor_.split("\\s*,\\s*")[2]);
        Integer iFingerID = Integer.valueOf(fingerTable.get(i_).nodeInfo.split("\\s*,\\s*")[2]);
        if (belongsToRange(successorID, (id - 1 + keySpace) % keySpace, iFingerID)
                && successorID >= fingerTable.get(i_).start) { //[)
            fingerTable.get(i_).nodeInfo = successor_;
            if (i_ == 0) {
                // update successor if needed
                successor = successor_;
            }
            String[] pred = predecessor.split("\\s*,\\s*");
            if (!String.join(",", pred).equals(successor_)) {
                // make sure predecessor is not the successor_ (avoid redundancy)
                // remote procedure call
                TTransport transport = new TSocket(pred[0], Integer.valueOf(pred[1]));
                transport.open();
                TProtocol protocol = new TBinaryProtocol(transport);
                NodeService.Client client = new NodeService.Client(protocol);
                client.updateFingerTable(successor_, i_);
                transport.close();
            }
        }
        System.out.print(getNodeDetails());
    }

    @Override
    public void updatePredecessor(String predecessor_) throws TException {
        System.out.printf("updatePredecessor(%s)\n", predecessor_);
        predecessor = predecessor_;
    }

    @Override
    public String set_(String bookTitle, String genre) throws TException {
        Integer key = intSHA1ModuloHash(bookTitle, keySpace);
        System.out.printf("set_(%s, %s) - %d\n", bookTitle, genre, key);
        String trail = address + "," + port + "," + id;
        if (belongsToRange(key, Integer.valueOf(predecessor.split("\\s*,\\s*")[2]),
                (id + 1) % keySpace)) { //(]
            // if the key is this node's responsibility
            bookTitleGenreMapping.put(bookTitle, genre);
            return trail;
        } else {
            // find the closest finger table entry to the key
            String[] nextNodeInfo = {};
            for (int i = mbits - 1; i > -1; i--) {
                if (belongsToRange(key, (fingerTable.get(i).start - 1 + keySpace) % keySpace,
                        fingerTable.get(i).end)) { // [)
                    nextNodeInfo = fingerTable.get(i).nodeInfo.split("\\s*,\\s*");
                }
            }
            TTransport transport = new TSocket(nextNodeInfo[0], Integer.valueOf(nextNodeInfo[1]));
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            NodeService.Client client = new NodeService.Client(protocol);
            trail += " -> " + client.set_(bookTitle, genre);
            transport.close();
            return trail;
        }
    }

    @Override
    public GetResponse get_(String bookTitle) throws TException {
        Integer key = intSHA1ModuloHash(bookTitle, keySpace);
        System.out.printf("get_(%s) - %d\n", bookTitle, key);
        String trail = address + "," + port + "," + id;
        if (belongsToRange(key, Integer.valueOf(predecessor.split("\\s*,\\s*")[2]),
                (id + 1) % keySpace)) { //(]
            // if the key is this node's responsibility
            String genre = bookTitleGenreMapping.get(bookTitle);
            return new GetResponse(genre, trail);
        } else {
            // find the closest finger table entry to the key
            String[] nextNodeInfo = {};
            for (int i = mbits - 1; i > -1; i--) {
                if (belongsToRange(key, (fingerTable.get(i).start - 1 + keySpace) % keySpace,
                        fingerTable.get(i).end)) { // [)
                    nextNodeInfo = fingerTable.get(i).nodeInfo.split("\\s*,\\s*");
                }
            }
            TTransport transport = new TSocket(nextNodeInfo[0], Integer.valueOf(nextNodeInfo[1]));
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            NodeService.Client client = new NodeService.Client(protocol);
            GetResponse getResponse = client.get_(bookTitle);
            getResponse.trail = trail + " -> " + getResponse.trail;
            transport.close();
            return getResponse;
        }
    }

    @Override
    public List<String> desc(int start_id) throws TException {
        System.out.printf("desc(%d)\n", start_id);
        List<String> detailList;
        String[] successorNodeInfo = successor.split("\\s*,\\s*");
        if(start_id == Integer.valueOf(successorNodeInfo[2])) {
            detailList = new ArrayList<>();
            detailList.add(getNodeDetails());
            return detailList;
        }
        TTransport transport = new TSocket(successorNodeInfo[0], Integer.valueOf(successorNodeInfo[1]));
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        NodeService.Client client = new NodeService.Client(protocol);
        detailList = client.desc(start_id);
        detailList.add(getNodeDetails());
        return detailList;
    }

    // Helper Methods
    private Boolean belongsToRange(int query, int rangeStart, int rangeEnd) {
        if ((rangeEnd - rangeStart + keySpace) % keySpace == 1) {
            // eg. (15,0), (3,4) then entire space becomes the range
            return true;
        }
        if (rangeStart < rangeEnd) {
            return (query > rangeStart && query < rangeEnd);
        } else if (rangeStart > rangeEnd) {
            return (query > rangeStart || query < rangeEnd);
        } else {
            // boundary condition
            return false;
        }
    }

    private static Integer intSHA1ModuloHash(String input, int modulo) {
        try {
            MessageDigest msgDigest = MessageDigest.getInstance("SHA-1");
            msgDigest.update(input.getBytes(StandardCharsets.UTF_8), 0, input.length());
            ByteBuffer bb = ByteBuffer.wrap(msgDigest.digest());
            return (int) Math.abs(bb.getLong() % modulo);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return -1;
        }
    }
}