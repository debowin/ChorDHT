struct JoinResponse {
	1: i32 id, // assigned id
	2: string nodeInfo // ip:port:id
}

struct GetResponse {
	1: string genre,
	2: string trail
}

service SuperNodeService {
	bool ping(),
	JoinResponse Join(1: string ip, 2: i32 port),
	void PostJoin(1: string ip, 2: i32 port), // done
	string GetNode() // nodeinfo
}

service NodeService {
    bool ping(),
    // start: navigation and location functions
    string findSuccessor(1: i32 id),
    string findPredecessor(1: i32 id),
    string findClosestPrecedingFinger(1: i32 id),
    // end
    // start: getters
    string getSuccessor(),
    string getPredecessor(),
    // end
    void updateFingerTable(1: string successor, 2: i32 i_),
    void updateSuccessor(1: string successor_),
    void updatePredecessor(1: string predecessor_),
    string set_(1: string bookTitle, 2: string genre), // return trail
    GetResponse get_(1: string bookTitle) // return genre, trail
}