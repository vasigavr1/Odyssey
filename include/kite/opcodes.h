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
#define RMW_ID_COMMITTED 36 // this is a 1-byte reply: it also tells me that the RMW has been committed in an older slot, and thus the issuer need not bcast commits
#define RMW_ID_COMMITTED_SAME_LOG 37 // this means I may need to broadcast commits, because the replier has not committed any RMWs in later log-slots
#define LOG_TOO_SMALL 38 // send the entire committed rmw
#define LOG_TOO_HIGH 39 // single byte-nack only proposes
#define SEEN_HIGHER_ACC 40 //both accs and props- send only TS different op than SEEN_HIGHER_PROP only for debug
// NO_OP_PROP_REP: Purely for debug: this is sent to proposes when an accept has been received
// for the same RMW-id and TS, that means the proposer will never see this opcode because
// it has already gathered prop reps quorum and sent accepts
#define NO_OP_PROP_REP 41
#define ACQ_LOG_TOO_SMALL 42
#define ACQ_LOG_TOO_HIGH 43
#define ACQ_LOG_EQUAL 44 // for acquires on rmws, the response is with respect to the log numbers

// this offset is added to the read reply opcode
// to denote that the machine doing the acquire was
// previously considered to have failed
#define FALSE_POSITIVE_OFFSET 30

// WRITE MESSAGE OPCODE
#define ONLY_WRITES 200 // could be write/accept/commit/release
#define ONLY_ACCEPTS 201
#define WRITES_AND_ACCEPTS 202


// RMW entry states for local and global entries
#define INVALID_RMW 0
#define PROPOSED 1 // has seen a propose || has been proposed
#define ACCEPTED 2 // has acked an accept || has fired accepts
#define NEEDS_KV_PTR 3  // there is already an entry for the key
#define RETRY_WITH_BIGGER_TS 4
#define MUST_BCAST_COMMITS 5 // locally committed-> must broadcast commits
// Broadcast Commits from helps in 2 occasions:
// 1. You are helping someone
// 2. You have received an already committed message
#define MUST_BCAST_COMMITS_FROM_HELP 6 // broadcast commits using the help_loc_entry as the source
#define COMMITTED 7 // Local entry only: bcasts broadcasted, but session not yet freed
#define TS_STALE_ON_REMOTE_KVS 8

/*
 *  SENT means the message has been sent
 *  READY means all acks have been gathered // OR a commit has been received
 * */

#define INVALID 0
#define VALID 1
#define SENT 2 // for reads
#define READY 3 // for reads
#define SENT_PUT 4 // typical writes -- ALL acks
#define SENT_RELEASE 5 // Release (could be the third round) -- All acks
#define SENT_ACQUIRE 6 //second round of acquire -- Quorum
#define SENT_COMMIT 7 // For commits -- All acks
#define SENT_RMW_ACQ_COMMIT 8 // -- Quorum
#define SENT_BIT_VECTOR 9 // -- Quorum
// Coalesced release that detected failure,
// but is behind other release that carries the bit vector
#define SENT_NO_OP_RELEASE 10 // -- Quorum


#define W_STATE_OFFSET 7
#define READY_PUT (SENT_PUT + W_STATE_OFFSET)
#define READY_RELEASE (SENT_RELEASE + W_STATE_OFFSET) // Release
#define READY_ACQUIRE (SENT_ACQUIRE + W_STATE_OFFSET) // second round of acquire
#define READY_COMMIT (SENT_COMMIT + W_STATE_OFFSET)
#define READY_RMW_ACQ_COMMIT (SENT_RMW_ACQ_COMMIT + W_STATE_OFFSET)
#define READY_BIT_VECTOR (SENT_BIT_VECTOR + W_STATE_OFFSET)
#define READY_NO_OP_RELEASE (SENT_NO_OP_RELEASE + W_STATE_OFFSET)


// Possible write sources
#define FROM_TRACE 0
#define FROM_READ 1
//#define RELEASE_SECOND 2 // after the read ts for a release
#define RELEASE_THIRD 3 // for the third round of a release
#define FOR_ACCEPT 4
#define FROM_ACQUIRE 5
#define FROM_COMMIT 6

// Possible flags when accepting locally
#define ACCEPT_ACK 1
#define NACK_ACCEPT_SEEN_HIGHER_TS 2
#define NACK_ACCEPT_LOG_OUT_OF_DATE 3
#define BROADCAST_COMMITS 4
#define DO_NOT_BROAD_CAST_COMMITS 5
#define ABORT_HELP 6
#define NACK_ALREADY_COMMITTED 7

// Possible Helping flags
#define NOT_HELPING 0
#define PROPOSE_NOT_LOCALLY_ACKED 1 // HELP from waiting too long
#define HELPING 2 // HELP to avoid deadlocks: The RMW metadata need not been stashed, because the help_loc_entry is in use
#define PROPOSE_LOCALLY_ACCEPTED 3 // Not needed, but used for readability
#define HELP_PREV_COMMITTED_LOG_TOO_HIGH 4
#define HELPING_MYSELF 5
//
#define ACCEPT_FLIPS_BIT_OP 128


// LOGGING for PAXOS
#define LOG_COMS 0
#define LOG_WS 1


#endif //KITE_OPCODES_H
