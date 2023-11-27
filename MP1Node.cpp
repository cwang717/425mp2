/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions. (Revised 2020)
 *
 *  Starter code template
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node( Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = new Member;
    this->shouldDeleteMember = true;
	memberNode->inited = false;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member* member, Params *params, EmulNet *emul, Log *log, Address *address) {
    for( int i = 0; i < 6; i++ ) {
        NULLADDR[i] = 0;
    }
    this->memberNode = member;
    this->shouldDeleteMember = false;
    this->emulNet = emul;
    this->log = log;
    this->par = params;
    this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {
    if (shouldDeleteMember) {
        delete this->memberNode;
    }
}

/**
* FUNCTION NAME: recvLoop
*
* DESCRIPTION: This function receives message from the network and pushes into the queue
*                 This function is called by a node to receive messages currently waiting for it
*/
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
* FUNCTION NAME: nodeStart
*
* DESCRIPTION: This function bootstraps the node
*                 All initializations routines for a member.
*                 Called by the application layer.
*/
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
    /*
    * This function is partially implemented and may require changes
    */
	// int id = *(int*)(&memberNode->addr.addr);
	// int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
	// node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
	initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == strcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
* FUNCTION NAME: finishUpThisNode
*
* DESCRIPTION: Wind up this node and clean up state
*/
int MP1Node::finishUpThisNode(){
    // for (auto& entry : memberNode->memberList) {
    //     Address addr = Address(to_string(entry.id) + ":" + to_string(entry.port));
    //     log->logNodeRemove(&memberNode->addr, &addr);
    // }

    return 0;
}

/**
* FUNCTION NAME: nodeLoop
*
* DESCRIPTION: Executed periodically at each member
*                 Check your messages in queue and perform membership protocol duties
*/
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
    MessageHdr *recMsgHdr = (MessageHdr *) data;
    if (recMsgHdr->msgType == JOINREQ) {
        return handleJoinReq(env, data, size);
    }
    if (recMsgHdr->msgType == JOINREP) {
        return handleJoinRep(env, data, size);
    }
    if (recMsgHdr->msgType == GOSSIP) {
        return handleGossip(env, data, size);
    }
    return false;
}

/**
* FUNCTION NAME: handleJoinReq
* 
* DESCRIPTION: Handle JOINREQ messages
*/
bool MP1Node::handleJoinReq(void* env, char *data, int size) {
    Address *senderAddr = (Address *) (data + sizeof(MessageHdr));
    int senderId = *(senderAddr->addr);
    short senderPort = *(senderAddr->addr + 4);
    long heartbeat = *(long *) (data + sizeof(MessageHdr) + 1 + sizeof(Address));

    // add the sender to the membership list
    MemberListEntry *newEntry = new MemberListEntry(senderId, senderPort, heartbeat, par->getcurrtime());
    memberNode->memberList.push_back(*newEntry);
    // if (memberNode->memberList.size() == par->MAX_NNB) {
    //     //remove the first element of the list
    //     memberNode->memberList.erase(memberNode->memberList.begin());
    // }
    log->logNodeAdd(&memberNode->addr, senderAddr);

    //reply a JOINREQ message containing the current membership vector back to the sender
    size_t msgsize = sizeof(MessageHdr) + sizeof(MemberListEntry) * memberNode->memberList.size();
    MessageHdr *msg = (MessageHdr *) malloc(msgsize * sizeof(char));
    msg->msgType = JOINREP;
    memcpy((char *)(msg+1), memberNode->memberList.data(), sizeof(MemberListEntry) * memberNode->memberList.size());
    
    emulNet->ENsend(&memberNode->addr, senderAddr, (char *)msg, msgsize);

    free(msg);
    
    return true;
}

/**
* FUNCTION NAME: handleJoinRep
*
* DESCRIPTION: Handle JOINREP messages 
*/

bool MP1Node::handleJoinRep(void* env, char *data, int size) {
    memberNode->inGroup = true;
    return handleGossip(env, data, size);
}

/**
* FUNCTION NAME: handleGossip
*
* DESCRIPTION: Handle GOSSIP messages
*/
bool MP1Node::handleGossip(void* env, char *data, int size) {
    vector<MemberListEntry> recvMemberList;
    size_t listLen = (size - sizeof(MessageHdr)) / sizeof(MemberListEntry);
    for (size_t i = 0; i < listLen; i++) {
        MemberListEntry *entry = (MemberListEntry *) (data + sizeof(MessageHdr) + i * sizeof(MemberListEntry));
        recvMemberList.push_back(*entry);
    }
    
    for (auto& recvEntry : recvMemberList) {
        bool found = false;
        for (auto& entry : memberNode->memberList) {
            if (recvEntry.id == entry.id && recvEntry.port == entry.port) {
                found = true;
                if (recvEntry.heartbeat > entry.heartbeat && par->getcurrtime() - entry.timestamp <= T_FAIL) {
                    entry.heartbeat = recvEntry.heartbeat;
                    entry.timestamp = par->getcurrtime();
                }
                break;
            }
        }
        if (!found) {
            memberNode->memberList.push_back(recvEntry);
            memberNode->memberList.back().timestamp = par->getcurrtime();
            Address addr = Address(to_string(recvEntry.id) + ":" + to_string(recvEntry.port));
            log->logNodeAdd(&memberNode->addr, &addr);
        }
    }

    return true;
}

/**
* FUNCTION NAME: nodeLoopOps
*
* DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
*                 the nodes
*                 Propagate your membership list
*/
void MP1Node::nodeLoopOps() {
    // update the heartbeat of the current node
    memberNode->heartbeat++;
    memberNode->memberList[0].heartbeat = memberNode->heartbeat;
    memberNode->memberList[0].timestamp = par->getcurrtime();

    for (auto it = memberNode->memberList.begin(); it != memberNode->memberList.end(); ) {
        if (par->getcurrtime() - it->timestamp > T_CLEAN) {
            Address addr = Address(to_string(it->id) + ":" + to_string(it->port));
            log->logNodeRemove(&memberNode->addr, &addr);
            it = memberNode->memberList.erase(it);
        } else {
            it++;
        }
    }

    // send the membership list to random fanout nodes
    vector<MemberListEntry> activeMemberList;
    for (auto& entry : memberNode->memberList) {
        if (par->getcurrtime() - entry.timestamp <= T_FAIL) {
            activeMemberList.push_back(entry);
        }
    }
    for (unsigned i = 0; i < fanout; i++) {
        // unsigned index = rand() % memberNode->memberList.size();
        // if (!index) {
        //     continue;
        // }
        // Address addr = Address(to_string(memberNode->memberList[index].id) + ":" + to_string(memberNode->memberList[index].port));
        int id = rand() % par->EN_GPSZ + 1;
        if (id == memberNode->memberList[0].id) {
            continue;
        }
        Address addr = Address(to_string(id) + ":" + to_string(par->PORTNUM));
        size_t msgsize = sizeof(MessageHdr) + sizeof(MemberListEntry) * activeMemberList.size();
        MessageHdr *msg = (MessageHdr *) malloc(msgsize * sizeof(char));
        msg->msgType = GOSSIP;
        memcpy((char *)(msg+1), activeMemberList.data(), sizeof(MemberListEntry) * activeMemberList.size());
        
        emulNet->ENsend(&memberNode->addr, &addr, (char *)msg, msgsize);
        // log->LOG(&memberNode->addr, (" send gossip to " + addr.getAddress()).c_str());

        free(msg);
    }

    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
    MemberListEntry *newEntry = new MemberListEntry(memberNode->addr.addr[0], memberNode->addr.addr[4], memberNode->heartbeat, par->getcurrtime());
    memberNode->memberList.push_back(*newEntry);
    log->logNodeAdd(&memberNode->addr, &memberNode->addr);
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}
