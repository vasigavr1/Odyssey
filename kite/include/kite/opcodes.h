//
// Created by vasilis on 27/04/20.
//

#ifndef KITE_OPCODES_H
#define KITE_OPCODES_H


#define OP_ACK 115
#define ACK_NOT_YET_SENT 117
#define OP_GET_TS 118 // first round of release, or out-of-epoch write
#define UPDATE_EPOCH_OP_GET 119



//KVS Response
//#define RETRY_RMW_NO_ENTRIES 0
#define RETRY_RMW 1
#define RMW_FAILURE 2 // when a CAS has to be cut short
#define RMW_SUCCESS 118




// READ_REPLIES
#define INVALID_OPCODE 5 // meaningless opcode to help with debugging
// an r_rep message can be a reply to a read or a prop or an accept
#define ACCEPT_REPLY_NO_CREDITS 24
#define ACCEPT_REPLY 25
#define PROP_REPLY 26 // Contains only prop reps
#define READ_REPLY 27 // Contains only read reps
#define READ_PROP_REPLY 127 // Contains read and prop reps

#define TS_TOO_HIGH 28
#define TS_EQUAL 29
#define TS_TOO_SMALL 30 // Response when reading the ts only (1st round of release)
//#define TS_GREATER 31
#define RMW_ACK 32 // 1 byte reply
#define RMW_ACK_ACC_SAME_RMW 33 // only for proposes: Have accepted with lower TS the same RMW-id, it counts as an ack
#define RMW_ACK_BASE_TS_STALE 33 // the propose is acked, but we let it know o f a recent ABD write
#define SEEN_HIGHER_PROP 34 // send that TS
#define SEEN_LOWER_ACC 35 // send value, rmw-id, TS
#define RMW_ID_COMMITTED 36 // this is a 1-byte reply: it also tells me that the RMW has been committed in an older slot, and thus the issuer need not bcast commits
#define RMW_ID_COMMITTED_SAME_LOG 37 // this means I may need to broadcast commits, because the replier has not committed any RMWs in later log-slots
#define LOG_TOO_SMALL 38 // send the entire committed rmw
#define LOG_TOO_HIGH 39 // single byte-nack only proposes
#define SEEN_HIGHER_ACC 40 //both accs and props- send only TS different op than SEEN_HIGHER_PROP only for debug
// NO_OP_PROP_REP: Purely for debug: this is sent to proposes when an accept has been received
// for the same RMW-id and TS, that means the proposer will never see this opcode because
// it has already gathered prop reps quorum and sent accepts
#define NO_OP_PROP_REP 41
#define CARTS_TOO_SMALL 42
#define CARTS_TOO_HIGH 43
#define CARTS_EQUAL 44

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
#define CAS_FAILED 8

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
//#define RELEASE_SECOND 2 // after the read base_ts for a release
#define RELEASE_THIRD 3 // for the third round of a release
#define FOR_ACCEPT 4
#define FROM_ACQUIRE 5
#define FROM_COMMIT 6

// Committing and writing the kv_ptr->value flags
enum {
  FROM_LOG_TOO_LOW_REP,
  FROM_ALREADY_COMM_REP,
  FROM_ALREADY_COMM_REP_HELP,
  FROM_LOCAL,
  FROM_LOCAL_HELP,
  FROM_REMOTE_COMMIT,
  FROM_REMOTE_COMMIT_NO_VAL,
  FROM_LOCAL_ACQUIRE,
  FROM_OOE_READ,
  //-- used only for writing kv_ptr->value
  FROM_TRACE_WRITE,
  FROM_BASE_TS_STALE,
  FROM_ISOLATED_OP,
  FROM_REMOTE_WRITE_RELEASE,
  FROM_OOE_LOCAL_WRITE
};




// Possible Helping flags
enum {
  NOT_HELPING,
  PROPOSE_NOT_LOCALLY_ACKED, // HELP from waiting too long
  HELPING, // HELP to avoid deadlocks: The RMW metadata need not been stashed, because the help_loc_entry is in use
  PROPOSE_LOCALLY_ACCEPTED, // Denotes that we are sending proposes for a locally accepted rmw
  HELP_PREV_COMMITTED_LOG_TOO_HIGH,
  HELPING_MYSELF,
  IS_HELPER
};

//
#define ACCEPT_FLIPS_BIT_OP 128


#define DO_NOT_CHECK_BASE_TS B_4_




// CONF BITS STATES
#define UP_STABLE 0
#define DOWN_STABLE 1
#define DOWN_TRANSIENT_OWNED 2
#define UNUSED_STATE 3 // used to denote that the field will not be used



// These are for when ENABLE_DEBUG_RMW_KV_PTR is activated
// possible flags explaining how the last committed RMW was committed
#define LOCAL_RMW 0
#define LOCAL_RMW_FROM_HELP 1
#define REMOTE_RMW 2
#define REMOTE_RMW_FROM_REP 3


// TS,version used
#define ALL_ABOARD_TS 2
#define PAXOS_TS 3

// LOGGING for PAXOS
#define LOG_COMS 0
#define LOG_WS 1


// VIRTUAL CHANNELS
#define VC_NUM 2
#define R_VC 0
#define W_VC 1


// QUEUE PAIRS
#define QP_NUM 4
#define R_QP_ID 0
#define R_REP_QP_ID 1
#define W_QP_ID 2
#define ACK_QP_ID 3

#define POLL_CQ_R 0
#define POLL_CQ_W 1
#define POLL_CQ_R_REP 2
#define POLL_CQ_ACK 3





enum {NO_REASON = 0, STALLED_BECAUSE_ACC_RELEASE = 1, STALLED_BECAUSE_NOT_ENOUGH_REPS};

#endif //KITE_OPCODES_H
