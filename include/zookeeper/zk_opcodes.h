//
// Created by vasilis on 23/06/2020.
//

#ifndef ZK_OPCODES_H
#define ZK_OPCODES_H


//Cache States
#define VALID_STATE 1
#define INVALID_STATE 2
#define INVALID_REPLAY_STATE 3
#define WRITE_STATE 4
#define WRITE_REPLAY_STATE 5

//Cache Opcode
//#define KVS_OP_GET 111
//#define KVS_OP_PUT 112
#define CACHE_OP_UPD 113
#define CACHE_OP_INV 114
#define KVS_OP_ACK 115
#define CACHE_OP_BRC 116       //Warning although this is cache opcode it's returned by cache to either Broadcast upd or inv

//Cache Response
//#define EMPTY 120
//#define CACHE_GET_SUCCESS 121
//#define CACHE_PUT_SUCCESS 122
//#define CACHE_UPD_SUCCESS 123
//#define CACHE_INV_SUCCESS 124
//#define CACHE_ACK_SUCCESS 125
//#define CACHE_LAST_ACK_SUCCESS 126
//#define RETRY 127

#define KEY_HIT 220


#define CACHE_MISS 130
#define CACHE_GET_STALL 131
#define CACHE_PUT_STALL 132
#define CACHE_UPD_FAIL 133
#define CACHE_INV_FAIL 134
#define CACHE_ACK_FAIL 135
#define CACHE_GET_FAIL 136
#define CACHE_PUT_FAIL 137

#define UNSERVED_CACHE_MISS 140


#endif //KITE_ZK_OPCODES_H
