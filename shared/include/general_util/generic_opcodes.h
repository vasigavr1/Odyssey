//
// Created by vasilis on 24/06/2020.
//

#ifndef KITE_GENERIC_OPCODES_H
#define KITE_GENERIC_OPCODES_H


typedef enum compare_t{SMALLER, EQUAL, GREATER, ERROR} compare_t;

// For latency measurements
typedef enum {
  NO_REQ,
  RELEASE,
  ACQUIRE,
  WRITE_REQ,
  READ_REQ,
  WRITE_REQ_BEFORE_CACHE
} req_type;


// CLIENT REQUESTS STATE
#define INVALID_REQ 0 // entry not being used
#define ACTIVE_REQ 1 // client has issued a reqs
#define IN_PROGRESS_REQ 2 // worker has picked up the req
#define COMPLETED_REQ 3 // worker has completed the req


enum {COMPARE_AND_SWAP_STRONG = 96,
  COMPARE_AND_SWAP_WEAK,
  FETCH_AND_ADD,
  RMW_PLAIN_WRITE // writes to rmwable keys get translated to this op
};


// when inserting the commit use this OP and change it to COMMIT_OP
// before broadcasting. The purpose is for the state of the commit message to be tagged as SENT_RMW_ACQ
// such that whens acks are gathered, it will be recognized that local entry need not get freed
#define COMMIT_OP_NO_VAL 100
#define RMW_ACQ_COMMIT_OP 101
#define COMMIT_OP 102
#define ACCEPT_OP 103
#define ACCEPT_OP_BIT_VECTOR 203
#define ACCEPT_OP_NO_CREDITS 13 // used only when creating an r_rep
#define PROPOSE_OP 104
#define OP_RELEASE_BIT_VECTOR 105// first round of a release that carries a bit vector
#define OP_RELEASE_SECOND_ROUND 106 // second round is the actual release
// The sender sends this opcode to flip a bit it owns after an acquire detected a failure
#define OP_ACQUIRE_FLIP_BIT 107 //the reply to this is always TS_EQUAL (not needed just for ease)
#define NO_OP_RELEASE 9 // on a coalesced Release which detected failure, but is behind an OP_RELEASE_BIT_VECTOR
#define OP_RELEASE 109
#define OP_ACQUIRE 110
// The receiver renames the opcode of an OP_ACQUIRE  to this to recognize
// that the acquire detected a failure and add the offset to the reply opcode
#define OP_ACQUIRE_FP 10
#define KVS_OP_GET 111
#define KVS_OP_PUT 112

//KVS Response
#define EMPTY 120
#define KVS_GET_TS_SUCCESS 21
#define KVS_GET_SUCCESS 121
#define KVS_PUT_SUCCESS 122
#define KVS_LOCAL_GET_SUCCESS 123
#define KVS_MISS 130


#endif //KITE_GENERIC_OPCODES_H
