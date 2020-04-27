//
// Created by vasilis on 27/04/20.
//

#ifndef KITE_OPCODES_H
#define KITE_OPCODES_H


enum {COMPARE_AND_SWAP_STRONG = 97,
  COMPARE_AND_SWAP_WEAK = 98,
  FETCH_AND_ADD  = 99,
  RMW_PLAIN_WRITE  =100 // writes to rmwable keys get translated to this op
};


// when inserting the commit use this OP and change it to COMMIT_OP
// before broadcasting. The purpose is for the state of the commit message to be tagged as SENT_RMW_ACQ
// such that whens acks are gathered, it will be recognized that local entry need not get freed
#define RMW_ACQ_COMMIT_OP 101
#define COMMIT_OP 102
#define ACCEPT_OP 103
#define ACCEPT_OP_BIT_VECTOR 203
#define ACCEPT_OP_NO_CREDITS 13 // used only when creating an r_rep
#define PROPOSE_OP 104
#define OP_RELEASE_BIT_VECTOR 105// first round of a release that carries a bit vector
#define OP_RELEASE_SECOND_ROUND 106 // second round is the actual release
// The sender sends this opcode to flip a bit it owns after an acquire detected a failure
#define OP_ACQUIRE_FLIP_BIT 107
#define NO_OP_RELEASE 9 // on a coalesced Release which detected failure, but is behind an OP_RELEASE_BIT_VECTOR
#define OP_RELEASE 109
#define OP_ACQUIRE 110
// The receiver renames the opcode of an OP_ACQUIRE  to this to recognize
// that the acquire detected a failure and add the offset to the reply opcode
#define OP_ACQUIRE_FP 10
#define KVS_OP_GET 111
#define KVS_OP_PUT 112


#define CACHE_OP_ACK 115
#define ACK_NOT_YET_SENT 117
#define CACHE_OP_GET_TS 118 // first round of release, or out-of-epoch write
#define UPDATE_EPOCH_OP_GET 119



//Cache Response
//#define RETRY_RMW_NO_ENTRIES 0
#define RETRY_RMW_KEY_EXISTS 1
#define RMW_FAILURE 2 // when a CAS has to be cut short
#define RMW_SUCCESS 118


#define EMPTY 120
#define KVS_GET_TS_SUCCESS 21
#define KVS_GET_SUCCESS 121
#define KVS_PUT_SUCCESS 122
#define KVS_LOCAL_GET_SUCCESS 123
#define KVS_MISS 130


// READ_REPLIES
#define INVALID_OPCODE 5 // meaningless opcode to help with debugging
// an r_rep message can be a reply to a read or a prop or an accept
#define ACCEPT_REPLY_NO_CREDITS 24
#define ACCEPT_REPLY 25
#define PROP_REPLY 26 // Contains only prop reps
#define READ_REPLY 27 // Contains only read reps
#define READ_PROP_REPLY 127 // Contains read and prop reps
#define TS_SMALLER 28
#define TS_EQUAL 29
#define TS_GREATER_TS_ONLY 30 // Response when reading the ts only (1st round of release)
#define TS_GREATER 31
#define RMW_ACK 32 // 1 byte reply
#define SEEN_HIGHER_PROP 33 // send that TS
#define SEEN_LOWER_ACC 34 // send value, rmw-id, TS
#define RMW_TS_STALE 35 // Ts was smaller than the KVS stored TS: send that TS
#define RMW_ID_COMMITTED 36 // send the entire committed rmw
#define LOG_TOO_SMALL 37 // send the entire committed rmw
#define LOG_TOO_HIGH 38 // single byte-nack only proposes
#define SEEN_HIGHER_ACC 39 //both accs and props- send only TS different op than SEEN_HIGHER_PROP only for debug
// NO_OP_PROP_REP: Purely for debug: this is sent to proposes when an accept has been received
// for the same RMW-id and TS, that means the proposer will never see this opcode because
// it has already gathered prop reps quorum and sent accepts
#define NO_OP_PROP_REP 40
#define ACQ_LOG_TOO_SMALL 41
#define ACQ_LOG_TOO_HIGH 42
#define ACQ_LOG_EQUAL 43 // for acquires on rmws, the response is with respect to the log numbers

// this offset is added to the read reply opcode
// to denote that the machine doing the acquire was
// previously considered to have failed
#define FALSE_POSITIVE_OFFSET 20

// WRITE MESSAGE OPCODE
#define ONLY_WRITES 200 // could be write/accept/commit/release
#define ONLY_ACCEPTS 201
#define WRITES_AND_ACCEPTS 202





#define IS_WRITE(X) (((X) == KVS_OP_PUT || (X) == OP_RELEASE) ? 1 : 0)





#endif //KITE_OPCODES_H
