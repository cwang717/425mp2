/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition (Revised 2020)
 *
 * MP2 Starter template version
 **********************************/
#include "MP2Node.h"

map<string, vector<pair<string, string>>> MP2Node::syncMap = {};

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
	this->delimiter = "::";
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
}

/**
* FUNCTION NAME: updateRing
*
* DESCRIPTION: This function does the following:
*                 1) Gets the current membership list from the Membership Protocol (MP1Node)
*                    The membership list is returned as a vector of Nodes. See Node class in Node.h
*                 2) Constructs the ring based on the membership list
*                 3) Calls the Stabilization Protocol
*/
void MP2Node::updateRing() {
	/*
     * Implement this. Parts of it are already implemented
     */
    vector<Node> curMemList;
    bool change = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());


	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
	bool changed = false;
	if (ring.size() != curMemList.size()) {
		changed = true;
	} else {
		for (unsigned i = 0; i < curMemList.size(); ++i) {
			if (ring[i].getHashCode() != curMemList[i].getHashCode()) {
				changed = true;
				break;
			}
		}
	}
	ring = curMemList;
	if (changed) {
		last_change = par->getcurrtime();
		return;
	}

	if (par->getcurrtime() - last_change == 10) {
		stabilizationProtocol();
	}
	
	if (par->getcurrtime() - last_change == 11) {
		stabilizationProtocol();
		last_change = -12;
	}
}

/**
* FUNCTION NAME: getMembershipList
*
* DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
*                 i) generates the hash code for each member
*                 ii) populates the ring member in MP2Node class
*                 It returns a vector of Nodes. Each element in the vector contain the following fields:
*                 a) Address of the node
*                 b) Hash code obtained by consistent hashing of the Address
*/
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
* FUNCTION NAME: hashFunction
*
* DESCRIPTION: This functions hashes the key and returns the position on the ring
*                 HASH FUNCTION USED FOR CONSISTENT HASHING
*
* RETURNS:
* size_t position on the ring
*/
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

/**
* FUNCTION NAME: clientCreate
*
* DESCRIPTION: client side CREATE API
*                 The function does the following:
*                 1) Constructs the message
*                 2) Finds the replicas of this key
*                 3) Sends a message to the replica
*/
void MP2Node::clientCreate(string key, string value) {
	/*
	* Implement this
	*/
	// find replicas
	vector<Node> replicas = findNodes(key);
	// send message to replicas
	Message recmsg = Message(++g_transID, memberNode->addr, CREATE, key, value);
	transMap.emplace(recmsg.transID, recmsg);
	for (unsigned i = 0; i < replicas.size(); i++) {
		Message msg = Message(g_transID, memberNode->addr, CREATE, key, value, ReplicaType(i));
		emulNet->ENsend(&memberNode->addr, replicas[i].getAddress(), msg.toString());
	}
}

/**
* FUNCTION NAME: clientRead
*
* DESCRIPTION: client side READ API
*                 The function does the following:
*                 1) Constructs the message
*                 2) Finds the replicas of this key
*                 3) Sends a message to the replica
*/
void MP2Node::clientRead(string key){
	/*
	* Implement this
	*/
	// find replicas
	vector<Node> replicas = findNodes(key);
	// send message to replicas
	Message recmsg = Message(++g_transID, memberNode->addr, READ, key);
	transMap.emplace(recmsg.transID, recmsg);
	for (unsigned i = 0; i < replicas.size(); i++) {
		Message msg = Message(g_transID, memberNode->addr, READ, key);
		emulNet->ENsend(&memberNode->addr, replicas[i].getAddress(), msg.toString());
	}
}

/**
* FUNCTION NAME: clientUpdate
*
* DESCRIPTION: client side UPDATE API
*                 The function does the following:
*                 1) Constructs the message
*                 2) Finds the replicas of this key
*                 3) Sends a message to the replica
*/
void MP2Node::clientUpdate(string key, string value){
	/*
    * Implement this
    */
   	// find replicas
	vector<Node> replicas = findNodes(key);
	// send message to replicas
	Message recmsg = Message(++g_transID, memberNode->addr, UPDATE, key, value);
	transMap.emplace(recmsg.transID, recmsg);
	for (unsigned i = 0; i < replicas.size(); i++) {
		Message msg = Message(g_transID, memberNode->addr, UPDATE, key, value);
		emulNet->ENsend(&memberNode->addr, replicas[i].getAddress(), msg.toString());
	}
}

/**
* FUNCTION NAME: clientDelete
*
* DESCRIPTION: client side DELETE API
*                 The function does the following:
*                 1) Constructs the message
*                 2) Finds the replicas of this key
*                 3) Sends a message to the replica
*/
void MP2Node::clientDelete(string key){
	/*
	* Implement this
	*/
	// find replicas
	vector<Node> replicas = findNodes(key);
	// send message to replicas
	Message recmsg = Message(++g_transID, memberNode->addr, DELETE, key);
	transMap.emplace(recmsg.transID, recmsg);
	for (unsigned i = 0; i < replicas.size(); i++) {
		Message msg = Message(g_transID, memberNode->addr, DELETE, key);
		emulNet->ENsend(&memberNode->addr, replicas[i].getAddress(), msg.toString());
	}
}

/**
* FUNCTION NAME: createKeyValue
*
* DESCRIPTION: Server side CREATE API
*                    The function does the following:
*                    1) Inserts key value into the local hash table
*                    2) Return true or false based on success or failure
*/
bool MP2Node::createKeyValue(int transID, string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// // Insert key, value, replicaType into the hash table
	// Entry entry(value, par->getcurrtime(), replica);
	// if (ht->create(key, entry.convertToString())) {
	if (ht->create(key, value)) {
		log->logCreateSuccess(&memberNode->addr, false, transID, key, value);
		return true;
	} else {
		log->logCreateFail(&memberNode->addr, false, transID, key, value);
		return false;
	}
		
}

/**
* FUNCTION NAME: readKey
*
* DESCRIPTION: Server side READ API
*                 This function does the following:
*                 1) Read key from local hash table
*                 2) Return value
*/
string MP2Node::readKey(int transID, string key) {
	/*
	 * Implement this
	 */
	// Read key from local hash table and return value
	string res = ht->read(key);
	if (!res.empty()) {
		log->logReadSuccess(&memberNode->addr, false, transID, key, res);
		return res;
	} else {
		log->logReadFail(&memberNode->addr, false, transID, key);
		return res;
	}
}

/**
* FUNCTION NAME: updateKeyValue
*
* DESCRIPTION: Server side UPDATE API
*                 This function does the following:
*                 1) Update the key to the new value in the local hash table
*                 2) Return true or false based on success or failure
*/
bool MP2Node::updateKeyValue(int transID, string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Update key in local hash table and return true or false
	if (ht->update(key, value)) {
		log->logUpdateSuccess(&memberNode->addr, false, transID, key, value);
		return true;
	} else {
		log->logUpdateFail(&memberNode->addr, false, transID, key, value);
		return false;
	}
}

/**
* FUNCTION NAME: deleteKey
*
* DESCRIPTION: Server side DELETE API
*                 This function does the following:
*                 1) Delete the key from the local hash table
*                 2) Return true or false based on success or failure
*/
bool MP2Node::deletekey(int transID, string key) {
	/*
	 * Implement this
	 */
	// Delete the key from the local hash table
	if (ht->deleteKey(key)) {
		log->logDeleteSuccess(&memberNode->addr, false, transID, key);
		return true;
	} else {
		log->logDeleteFail(&memberNode->addr, false, transID, key);
		return false;
	}
}

/**
* FUNCTION NAME: checkMessages
*
* DESCRIPTION: This function is the message handler of this node.
*                 This function does the following:
*                 1) Pops messages from the queue
*                 2) Handles the messages according to message types
*/
void MP2Node::checkMessages() {
	/*
	* Implement this. Parts of it are already implemented
	*/
	char * data;
	int size;

	/*
	* Declare your local variables here
	*/
	vector<string> readValues = {};

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);
		Message msg(message);
		/*
		 * Handle the message types here
		 */
		switch (msg.type) {
			case CREATE: 
			{
				bool succ = createKeyValue(msg.transID, msg.key, msg.value, msg.replica);
				Message reply = Message(msg.transID, memberNode->addr, REPLY, succ);
				emulNet->ENsend(&memberNode->addr, &msg.fromAddr, reply.toString());
				break;
			}
			case READ:
			{
				string res = readKey(msg.transID, msg.key);
				Message reply = Message(msg.transID, memberNode->addr, READREPLY, msg.key, res);
				emulNet->ENsend(&memberNode->addr, &msg.fromAddr, reply.toString());
				break;
			}
			case UPDATE:
			{
				bool succ = updateKeyValue(msg.transID, msg.key, msg.value, msg.replica);
				Message reply = Message(msg.transID, memberNode->addr, REPLY, succ);
				emulNet->ENsend(&memberNode->addr, &msg.fromAddr, reply.toString());
				break;
			}
			case DELETE:
			{
				bool succ = deletekey(msg.transID, msg.key);
				Message reply = Message(msg.transID, memberNode->addr, REPLY, succ);
				emulNet->ENsend(&memberNode->addr, &msg.fromAddr, reply.toString());
				break;
			}
			case REPLY:
			{
				if (msg.success) {
					recRecord[msg.transID]++;
				} else {
					if (recRecord.find(msg.transID) == recRecord.end()) {
						recRecord.emplace(msg.transID, 0);
					}
				}
				break;
			}
			case READREPLY:
			{
				if (msg.value.empty()) {
					if (recRecord.find(msg.transID) == recRecord.end()) {
						recRecord.emplace(msg.transID, 0);
					}
				} else {
					recRecord[msg.transID]++;
					readValues.emplace_back(msg.value);
				}
				break;
			}
		}
	}

	/*
	* This function should also ensure all READ and UPDATE operation
	* get QUORUM replies
	*/
	for (auto & item : recRecord) {
		auto & msg = transMap.at(item.first);
		switch (msg.type) {
			case CREATE: {
				if (item.second >= 2) {
					log->logCreateSuccess(&memberNode->addr, true, msg.transID, msg.key, msg.value);
				} else {
					log->logCreateFail(&memberNode->addr, true, msg.transID, msg.key, msg.value);
				}
				break;
			}
			case UPDATE:
			{
				if (item.second >= 2) {
					log->logUpdateSuccess(&memberNode->addr, true, msg.transID, msg.key, msg.value);
				} else {
					log->logUpdateFail(&memberNode->addr, true, msg.transID, msg.key, msg.value);
				}
				break;
			}
			case DELETE: {
				if (item.second >= 2) {
					log->logDeleteSuccess(&memberNode->addr, true, msg.transID, msg.key);
				} else {
					log->logDeleteFail(&memberNode->addr, true, msg.transID, msg.key);
				}
				break;
			}
			case READ: {
				if (item.second >= 2) {
					bool allSame = true;
					// static char stdstring[1000];
					// sprintf(stdstring, readValues[0]);
					// log->LOG(&memberNode->addr, stdstring);
					// for (unsigned i = 1; i < readValues.size(); ++i) {
					// 	if (readValues[i] != readValues[0]) {
					// 		allSame = false;
					// 		break;
					// 	}
					// }
					if (allSame) {
						log->logReadSuccess(&memberNode->addr, true, msg.transID, msg.key, readValues[0]);
					} else {
						log->logReadFail(&memberNode->addr, true, msg.transID, msg.key);
					}
				} else {
					log->logReadFail(&memberNode->addr, true, msg.transID, msg.key);
				}
			}
		}
	}

	recRecord.clear();
			
}

/**
* FUNCTION NAME: findNodes
*
* DESCRIPTION: Find the replicas of the given keyfunction
*                 This function is responsible for finding the replicas of a key
*/
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i < (int)ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
	if ( memberNode->bFailed ) {
		return false;
	}
	else {
		return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
	}
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
* FUNCTION NAME: stabilizationProtocol
*
* DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
*                 It ensures that there always 3 copies of all keys in the DHT at all times
*                 The function does the following:
*                1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
*                Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
*/
void MP2Node::stabilizationProtocol() {
	/*
	* Implement this
	*/
	vector<string> toDelete;
	for (auto & item : ht->hashTable) {
		bool shouldDelete = true;
		vector<Node> replicas = findNodes(item.first);
		for (unsigned i = 0; i < replicas.size(); ++i) {
			auto &replica = replicas[i];
			if (replica.getAddress()->getAddress() == memberNode->addr.getAddress()) {
				shouldDelete = false;
			} else {
				if (syncMap.find(replica.getAddress()->getAddress()) == syncMap.end()) {
					syncMap[replica.getAddress()->getAddress()] = {make_pair(item.first, item.second)};
				} else {
					syncMap[replica.getAddress()->getAddress()].emplace_back(make_pair(item.first, item.second));
				}
			}
		}
		if (shouldDelete) {
			toDelete.emplace_back(item.first);
		}
	}

	for (auto & key : toDelete) {
		ht->deleteKey(key);
	}

	for (auto iter = syncMap.begin(); iter != syncMap.end();) {
		if (iter->first == memberNode->addr.getAddress()) {
			for (auto & item : iter->second) {
				ht->create(item.first, item.second);
			}
			iter = syncMap.erase(iter);
		} else {
			++iter;
		}
		
	}
}
