//
// Created by vasilis on 30/06/20.
//

#ifndef INLINE_UTIL_H
#define INLINE_UTIL_H

#include "debug_util.h"
#include "client_if_util.h"
#include "latency_util.h"
#include "rdma_gen_util.h"
#include "generic_inline_util.h"
#include "config_util.h"



static inline void create_inputs_of_op(uint8_t **value_to_write, uint8_t **value_to_read,
                                       uint32_t *real_val_len, uint8_t *opcode,
                                       uint32_t *index_to_req_array,
                                       mica_key_t *key, uint8_t *op_value, trace_t *trace,
                                       int working_session, uint16_t t_id)
{
  client_op_t *if_cl_op = NULL;
  if (ENABLE_CLIENTS) {
    uint32_t pull_ptr = interface[t_id].wrkr_pull_ptr[working_session];
    if_cl_op = &interface[t_id].req_array[working_session][pull_ptr];
    (*index_to_req_array) = pull_ptr;
    check_client_req_state_when_filling_op(working_session, pull_ptr,
                                           *index_to_req_array, t_id);

    (*opcode) = if_cl_op->opcode;
    (*key) = if_cl_op->key;
    (*real_val_len) = if_cl_op->val_len;
    (*value_to_write) = if_cl_op->value_to_write;
    (*value_to_read) = if_cl_op->value_to_read;
  }
  else {
    (*opcode) = trace->opcode;
    *(uint64 *) (key) = *(uint64 *) trace->key_hash;
    (*real_val_len) = (uint32_t) VALUE_SIZE;
    (*value_to_write) = op_value;
    (*value_to_read) = op_value;
    if (*opcode == FETCH_AND_ADD) *(uint64_t *) op_value = 1;
  }

}


#endif //INLINE_UTIL_H
