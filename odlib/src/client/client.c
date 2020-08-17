//
// Created by vasilis on 05/02/19.
//

#include "interface.h"
#include "trace_util.h"
#include "kvs.h"


#define CLIENT_ASSERTIONS 1
#define NUM_OF_RMW_KEYS 50000

/* --------------------------------------------------------------------------------------
 * ----------------------------------TRACE-----------------------------------------------
 * --------------------------------------------------------------------------------------*/

// Use a trace - can be either manufactured or from text
static inline uint32_t send_reqs_from_trace(trace_t *trace, uint16_t t_id)
{
  const uint16_t worker_num = (uint16_t)(WORKERS_PER_MACHINE / CLIENTS_PER_MACHINE_);
  uint16_t first_worker = worker_num * t_id;
  uint16_t last_worker = (uint16_t) (first_worker + worker_num - 1);
  uint32_t dbg_cntr = 0;
  uint32_t trace_ptr = 0;
  uint8_t* value = (uint8_t *) calloc((size_t) VALUE_SIZE, 1);
  bool *cas_result = (bool*) calloc(1, sizeof(bool));
  while (true) {
    uint16_t w_i = 0, s_i = 0;
    bool polled = false;
    // poll requests
    for (w_i = 0; w_i < worker_num; w_i++) {
      uint16_t wrkr = w_i + first_worker;
      for (s_i = 0; s_i < SESSIONS_PER_THREAD; s_i++) {
        uint16_t pull_ptr = interface[wrkr].clt_pull_ptr[s_i];
        //printf("Client %u  wrkr %u s_i %u, w_pull_ptr %u \n", t_id, wrkr, s_i, w_pull_ptr);
        while (interface[wrkr].req_array[s_i][pull_ptr].state == COMPLETED_REQ) {
          // get the result
          polled = true;
          if (CLIENT_DEBUG)
            my_printf(green, "Client %u pulling req from worker %u for session %u, slot %u\n",
                         t_id, wrkr, s_i, pull_ptr);
          atomic_store_explicit(&interface[wrkr].req_array[s_i][pull_ptr].state, INVALID_REQ, memory_order_relaxed);
          MOD_INCR(interface[wrkr].clt_pull_ptr[s_i], PER_SESSION_REQ_NUM);
        }
      }
    }
    if (!polled) dbg_cntr++;
    else dbg_cntr = 0;
    if (dbg_cntr == BILLION) {
      printf("Failed to poll \n");
      //interface[wrkr].clt_pull_ptr[s_i], (void *)&req_array[w_i][s_i][interface[wrkr].clt_pull_ptr[s_i]].state, req_array[w_i][s_i][w_pull_ptr[w_i][s_i]].state);
      dbg_cntr = 0;
    }
    // issue requests
    for (w_i = 0; w_i < worker_num; w_i++) {
      uint16_t wrkr = w_i + first_worker;
      for (s_i = 0; s_i < SESSIONS_PER_THREAD; s_i++) {
        uint16_t push_ptr = interface[wrkr].clt_push_ptr[s_i];
        while (interface[wrkr].req_array[s_i][push_ptr].state == INVALID_REQ) {
          if (CLIENT_DEBUG)
            my_printf(yellow, "Client %u inserting req to worker %u for session %u, in slot %u from trace slot %u ptr %p\n",
                          t_id, wrkr, s_i, push_ptr, trace_ptr, &interface[wrkr].req_array[s_i][push_ptr].state);
          interface[wrkr].req_array[s_i][push_ptr].opcode = trace[trace_ptr].opcode;
          memcpy(&interface[wrkr].req_array[s_i][push_ptr].key, trace[trace_ptr].key_hash, KEY_SIZE);
          interface[wrkr].req_array[s_i][push_ptr].val_len = (uint32_t) VALUE_SIZE;
          interface[wrkr].req_array[s_i][push_ptr].value_to_read = value;
          interface[wrkr].req_array[s_i][push_ptr].rmw_is_successful = cas_result;
          atomic_store_explicit(&interface[wrkr].req_array[s_i][push_ptr].state, (uint8_t) ACTIVE_REQ,
                                memory_order_release);
          MOD_INCR(interface[wrkr].clt_push_ptr[s_i], PER_SESSION_REQ_NUM);
          trace_ptr++;
          if (trace[trace_ptr].opcode == NOP) trace_ptr = 0;
        }
      }
    }
  }
  return trace_ptr;
}



/* ------------------------------------------------------------------------------------------------------------------- */
/* -----------------------------------SIMPLE TESTING------------------------------------------------------------------ */
/* ------------------------------------------------------------------------------------------------------------------- */
#define RELAXED_WRITES 5
static inline void rel_acq_circular_blocking() {
  int i = 0;
  uint8_t expected_val[VALUE_SIZE] = {0};
  uint8_t desired_val[VALUE_SIZE] = {0};
  uint8_t value[VALUE_SIZE] = {0};
  int key_id = 0;
  int expected_int, desired_int;
  uint16_t session_id = 0;
  bool cas_result;
  sleep(3);
  uint32_t flag_offset = 2 * NUM_OF_RMW_KEYS + (MACHINE_NUM * 100);
  uint32_t key_flags[MACHINE_NUM];
  for (i = 0; i < MACHINE_NUM; i++) key_flags[i] = flag_offset + i;
  uint32_t key_offset = (uint32_t) (NUM_OF_RMW_KEYS);
  uint16_t relaxed_writes = 5;
  uint32_t val_len = 20;

  // machine 0 kicks things off
  if (machine_id == 0) {
    for (i = 0; i < relaxed_writes; i++) {
      desired_val[0] = (uint8_t) (10 * machine_id + i);
      blocking_write(key_offset + i, desired_val, val_len, session_id);
      my_printf(yellow, "Writing key %u, iteration %u, value %u \n", key_offset + i, i, desired_val[0]);
    }
    desired_val[0] = 1;
    blocking_release(key_flags[machine_id], desired_val, val_len, session_id);
  }

  uint8_t prev_machine_id = (uint8_t) ((MACHINE_NUM + machine_id - 1) % MACHINE_NUM);
  while (true) {
    // First acquire the previous machine flag
    my_printf(yellow, "Machine %d Acquiring key_flag %u  from machine %u\n",
                  machine_id, key_flags[prev_machine_id], prev_machine_id);
    do {
      int ret = blocking_acquire(key_flags[prev_machine_id], value, val_len, session_id);
      assert(ret > 0);
    } while (value[0] != 1);

    my_printf(yellow, "Machine %d Acquired key_flag %u  from machine %u\n",
                  machine_id, key_flags[prev_machine_id], prev_machine_id);
    // Then read all values to make sure they are what they are supposed to be
    for (i = 0; i < relaxed_writes; i++) {
      blocking_read(key_offset + i, value, val_len, session_id);
      my_printf(cyan, "Reading key %u, iteration %u, value %u \n", key_offset + i, i, value[0]);
      assert(value[0] == 10 * prev_machine_id + i);
    }

    // Write the same values but with different values
    for (i = 0; i < relaxed_writes; i++) {
      desired_val[0] = (uint8_t) (10 * machine_id + i);
      blocking_write(key_offset + i, desired_val, val_len, session_id);
    }

    // reset the flag of the previous machine
    desired_val[0] = 0;
    blocking_write(key_flags[prev_machine_id], desired_val, val_len, session_id);

    // release your flag
    desired_val[0] = 1;
    my_printf(yellow, "Releasing key_flag %u \n", key_flags[machine_id]);
    blocking_release(key_flags[machine_id], desired_val, val_len, session_id);
  }
}

static inline void rel_acq_circular_async() {
  int i = 0;
  uint8_t expected_val[VALUE_SIZE] = {0};
  uint8_t desired_val[VALUE_SIZE] = {0};
  uint8_t value[RELAXED_WRITES][VALUE_SIZE] = {0};
  int key_id = 0;
  int expected_int, desired_int;
  uint16_t session_id = 0;
  bool cas_result;
  sleep(3);
  uint32_t flag_offset = 2 * NUM_OF_RMW_KEYS + (MACHINE_NUM * 100);
  uint32_t key_flags[MACHINE_NUM];
  for (i = 0; i < MACHINE_NUM; i++) key_flags[i] = flag_offset + i;
  //uint32_t key_offset = (uint32_t) (NUM_OF_RMW_KEYS);
  uint8_t prev_machine_id = (uint8_t) ((MACHINE_NUM + machine_id - 1) % MACHINE_NUM);
  // Write keys depending on my machine_id
  //uint32_t write_key_offset = (uint32_t) (NUM_OF_RMW_KEYS + machine_id * RELAXED_WRITES);
  //uint32_t read_key_offset = (uint32_t) (NUM_OF_RMW_KEYS + prev_machine_id * RELAXED_WRITES);
  uint32_t write_key_offset = (uint32_t) (NUM_OF_RMW_KEYS);
  uint32_t read_key_offset = write_key_offset;
  uint16_t relaxed_writes = RELAXED_WRITES;
  uint64_t last_issued_req = 0;
  int ret;
  uint val_len = 1;

  // machine 0 kicks things off
  if (machine_id == 0) {
    for (i = 0; i < relaxed_writes; i++) {
      desired_val[0] = (uint8_t) (10 * machine_id + i);
      async_write_strong(write_key_offset + i, desired_val, val_len, session_id);
      //my_printf(yellow, "Writing key %u, iteration %u, value %u \n", write_key_offset + i, i, desired_val[0]);
    }
    desired_val[0] = 1;
    async_release_strong(key_flags[machine_id], desired_val, val_len, session_id);
  }
  while (true) {
    // First acquire the previous machine flag
    //my_printf(yellow, "Machine %d Acquiring key_flag %u  from machine %u\n",
    //              machine_id, key_flags[prev_machine_id], prev_machine_id);
    do {
      ret = blocking_acquire(key_flags[prev_machine_id], value[0], val_len, session_id);
      assert(ret >= 0);
    } while (value[0][0] != 1);

    //my_printf(yellow, "Machine %d Acquired key_flag %u  from machine %u\n",
    //              machine_id, key_flags[prev_machine_id], prev_machine_id);


    // Issue asynchronous reads
    for (i = 0; i < relaxed_writes; i++) {
      last_issued_req = (uint64_t) async_read_strong(read_key_offset + i, value[i], val_len, session_id);
    }


    // Write different values but with different values
    for (i = 0; i < relaxed_writes; i++) {
      desired_val[0] = (uint8_t) (10 * machine_id + i);
      ret = async_write_strong(write_key_offset + i, desired_val, val_len, session_id);
      assert(ret > 0);
    }

    // reset the flag of the previous machine
    desired_val[0] = 0;
    ret = async_write_strong(key_flags[prev_machine_id], desired_val, val_len, session_id);
    assert(ret > 0);

    // release your flag
    desired_val[0] = 1;
    //my_printf(yellow, "Releasing key_flag %u \n", key_flags[machine_id]);
    ret = async_release_strong(key_flags[machine_id], desired_val, val_len, session_id);
    assert(ret > 0);

    // Do the actual reads
    poll_a_req_blocking(session_id, last_issued_req);
    for (i = 0; i < relaxed_writes; i++) {
      //my_printf(cyan, "Reading key %u, iteration %u, value %u \n", read_key_offset + i, i, value[i][0]);
      assert(value[i][0] == 10 * prev_machine_id + i);
    }

  }
}

/* ------------------------------------------------------------------------------------------------------------------- */
/*-------------------------------------- TREIBER STACK---------------------------------------------------------------- */
/* ------------------------------------------------------------------------------------------------------------------- */


#define TR_INIT 0
// TREIBER PULL STATES
#define TR_READ_TOP 1
#define TR_READ_FIRST 2
// TREIBER PUSH or PULL STATE
#define PUSHING 0
#define POPPING 1


// TREIBER STRUCTS
struct top {
  uint32_t fourth_key_id;
  uint32_t third_key_id;
  uint32_t sec_key_id;
  uint32_t key_id;
  uint32_t pop_counter;
  uint32_t push_counter;
};
#define NODE_SIZE (VALUE_SIZE - 17)
#define NODE_SIGNATURE 144
struct node {
  uint8_t value[NODE_SIZE];
  bool pushed;
  uint16_t stack_id;
  uint16_t owner;
  uint32_t push_counter;
  uint32_t key_id;
  uint32_t next_key_id;
};

#define NUMBER_OF_STACKS (GLOBAL_SESSION_NUM)
#define TR_KEY_OFFSET NUMBER_OF_STACKS
#define MAX_TR_NODE_KEY ((GLOBAL_SESSION_NUM * TREIBER_WRITES_NUM) + TR_KEY_OFFSET)
#define DEBUG_MAX 100


struct tr_sess_info_dbg {
  uint8_t state;
  uint8_t push_or_pull_state;
  bool success;
  bool valid_key_to_write;
  bool done_cas;
  uint16_t s_i;
  uint16_t real_sess_i;
  uint16_t glob_sess_i;
  uint16_t wrkr;
  uint32_t last_req_id;
  uint32_t stack_id;
  uint32_t owned_key;
  uint32_t pop_dbg;
  uint32_t push_dbg;
  struct top top;
  struct top new_top;
  struct top pop_top;
  struct top pop_new_top;
  struct node *new_node;
  struct node *owned_node;
};



static inline bool check_top(struct top *top, const char *message,
                             uint32_t stack_id)
{
  if (ENABLE_ASSERTIONS) {
    bool silent = strcmp(message, "Pop-new_top before CAS ") == 0;
    //assert(top->push_counter >= top->pop_counter);

    if (top->push_counter == top->pop_counter) {
      if (top->key_id != 0) { // Stack must be empty
        if (!silent) my_printf(red, "%s: Stack %u should be empty: pushed %u, popped %u pointer %u \n",
                               message, stack_id, top->push_counter, top->pop_counter, top->key_id);
        return false;
        assert(false);
      }
    } else if (top->push_counter > top->pop_counter) {
      if (top->key_id < TR_KEY_OFFSET) { // Stack cannot be empty
        if (!silent)  my_printf(red, "%s: Stack %u cannot be empty: pushed %u, popped %u pointer %u \n",
                                message, stack_id, top->push_counter, top->pop_counter, top->key_id);
        return false;
        assert(false);
      }
    }
  }
  return true;
}


static inline bool check_treiber_values(uint8_t *old_val, uint8_t *new_val)
{
  struct top *new_top = (struct top *) new_val;
  struct top *old_top = (struct top *) old_val;

  if (new_top->push_counter < old_top->push_counter) {
    my_printf(red, "New-push/old-push %u/%u \n",
              new_top->push_counter , old_top->push_counter);
    return false;
  }
  if (new_top->pop_counter < old_top->pop_counter) {
    my_printf(red, "New-pop/old-pop %u/%u \n",
              new_top->pop_counter , old_top->pop_counter);
    return false;
  }
  return true;

}






static inline void update_file(uint16_t t_id, uint32_t key_id, struct tr_sess_info_dbg *info, bool push,
                               struct top *top, struct top * new_top)
{
  if (CLIENT_LOGS) {
    uint64_t key_hash = CityHash128((char *) &(key_id), 4).second;
    struct key key;
    memcpy(&key, &key_hash, KEY_SIZE);

//    fprintf(client_log[t_id],
//            "%s: Stack: %u key: %u/%u, new_top key_id %u  push counter: %u pop counter: %u \n",
//            push ? "Push" : "Pull", info->stack_id, key.bkt, key_id, new_top->key_id,
//            new_top->push_counter,
//            new_top->pop_counter);
    uint16_t glob_sess = (uint16_t) (machine_id * SESSIONS_PER_MACHINE + info->real_sess_i);

//    fprintf(client_log[t_id],
//            "%u %u %s %u %u %u %u\n",
//            key.bkt, glob_sess, push ? "Push" : "Pull", info->stack_id,  new_top->push_counter,
//            new_top->pop_counter, key_id);
    fprintf(client_log[t_id],
            "%u %u \n",
            info->stack_id, new_top->pop_counter);

  }

  //fprintf(client_log[t_id], "Push: Stack: %u key: %u  push counter: %u pop counter: %u \n",
  //        info->stack_id, new_top->key_id, new_top->push_counter, new_top->pop_counter);

}



static inline bool check_top_success(struct top *top, struct top *new_top)
{
  assert(top != NULL); assert(new_top != NULL);
  bool pushing = new_top->push_counter == top->push_counter + 1;
  bool popping = new_top->pop_counter == top->pop_counter + 1;
  assert(pushing || popping);
  if (pushing) assert(new_top->pop_counter == top->pop_counter);
  if (popping) assert(new_top->push_counter == top->push_counter);
}

static inline bool check_tops_equal(struct top *top, struct top *comp_top)
{
  assert(top != NULL); assert(comp_top != NULL);
  assert(top->key_id == comp_top->key_id &&
         top->push_counter == comp_top->push_counter &&
         top->pop_counter == comp_top->pop_counter);
}

static inline bool are_tops_equal(struct top *top, struct top *comp_top)
{
  assert(top != NULL); assert(comp_top != NULL);
  return (top->key_id == comp_top->key_id &&
          top->push_counter == comp_top->push_counter &&
          top->pop_counter == comp_top->pop_counter);
}

static inline void check_node(uint8_t *val, char *message,
                              uint32_t key_bkt)
{
  if (ENABLE_ASSERTIONS) {
    struct node *node = (struct node *) val;
    if (node->next_key_id == 0) return;
    if (node->next_key_id >= TR_KEY_OFFSET && node->next_key_id <= MAX_TR_NODE_KEY)
      return;

    my_printf(red, "%s node key id %u, bkt %u\n",
               message, node->next_key_id, key_bkt);
    assert(false);
  }
}


static inline void treiber_push_blocking(uint16_t session_id, uint32_t stack_id, int key_id_to_push)
{
  if (key_id_to_push < 0) {
    my_printf(red, "Tried to push a negative key id %d \n", key_id_to_push);
    return;
  }
  uint32_t new_node_key = (uint32_t) key_id_to_push;
  uint32_t top_key_id = stack_id;
  assert(top_key_id < TR_KEY_OFFSET);
  assert(key_id_to_push >= TR_KEY_OFFSET);
  assert(key_id_to_push < KVS_NUM_KEYS);
  assert(session_id < SESSIONS_PER_MACHINE);
  assert(sizeof(struct top) <= VALUE_SIZE);
  assert(sizeof(struct node) == VALUE_SIZE);

  struct top top, new_top;
  struct node new_node;
  bool success = false;
  blocking_read(top_key_id, (uint8_t *)&top, sizeof(struct top), session_id);
  //my_printf(green, "Read top_key_id %u, points to key %u top counter %u\n",
  //              stack_id, top.key_id, top.counter);

  do {
    new_node.next_key_id = top.key_id;
    async_write_strong(new_node_key, (uint8_t *)&new_node, sizeof(struct node), session_id);
    new_top.pop_counter = top.pop_counter;
    new_top.key_id = new_node_key;
    blocking_cas(top_key_id, (uint8_t *)&top, (uint8_t *)&new_top, sizeof(struct top),
                 &success, true, session_id);
//    if (!success) {
//      printf("top.key %u, top.counter %lu success %d \n", top.key_id, top.counter, success);
//      assert(false);
//    }
  } while(!success);

//  my_printf(green, "Session %u successfully pushed key %u to stack %u, previous top key %u top counter %u\n",
//               session_id, new_node_key, stack_id, top.key_id, top.counter);
}

static inline int treiber_pop_blocking(uint16_t session_id, uint32_t stack_id)
{
  uint32_t top_key_id = stack_id;
  assert(top_key_id < TR_KEY_OFFSET);
  assert(session_id < SESSIONS_PER_MACHINE);
  assert(sizeof(struct top) <= VALUE_SIZE);
  assert(sizeof(struct node) == VALUE_SIZE);

  struct top top, new_top;
  struct node first_node;
  bool success = false;
  blocking_read(top_key_id, (uint8_t *)&top, sizeof(struct top), session_id);
  //my_printf(green, "Read top_key_id %u, points to key %u top counter %u\n",
  //              stack_id, top.key_id, top.counter);

  do {
    if (top.key_id == 0) return - 1;
    blocking_read(top.key_id, (uint8_t *)&first_node, sizeof(struct node), session_id);
    //async_write_strong(new_node_key, (uint8_t *)&new_node, sizeof(struct node), session_id);
    new_top.pop_counter = top.pop_counter + 1;
    new_top.key_id = first_node.next_key_id;
    blocking_cas(top_key_id, (uint8_t *)&top, (uint8_t *)&new_top, sizeof(struct top),
                 &success, true, session_id);
//    if (!success) {
//      printf("top.key %u, top.counter %lu success %d \n", top.key_id, top.counter, success);
//      assert(false);
//    }
  } while(!success);


//  my_printf(green, "Session %u successfully popped key %u from stack %u, top counter %u\n",
//               session_id, top.key_id, stack_id, top.counter);
  return top.key_id;
}



static inline void treiber_push_pop_blocking()
{
  uint32_t key_offset = (uint32_t) (NUM_OF_RMW_KEYS + (NUM_OF_RMW_KEYS * machine_id));
  uint32_t stack_id = 0;//machine_id;
  for (int i = 0; i < 10; i++) {
    treiber_push_blocking(0, stack_id, key_offset + i);
  }
  while (true) {
    for (int i = 0; i < NUM_OF_RMW_KEYS; i++) {
      uint32_t key_to_push = (uint32_t) treiber_pop_blocking(0, stack_id);
      treiber_push_blocking(0, stack_id, key_to_push);
    }
  }
}







static inline void treiber_pop_multi_session(uint16_t t_id)
{

  assert(sizeof(struct top) <= VALUE_SIZE);
  assert(sizeof(struct node) == VALUE_SIZE);
  uint16_t s_i = 0;
  uint16_t sess_offset = (uint16_t) (t_id * SESSIONS_PER_CLIENT);
  assert(sess_offset + SESSIONS_PER_CLIENT <= SESSIONS_PER_MACHINE);
  //uint32_t stack_ids[SESSIONS_PER_MACHINE];
  struct top top[SESSIONS_PER_CLIENT], new_top[SESSIONS_PER_CLIENT];
  struct node first_node[SESSIONS_PER_CLIENT];
  bool success[SESSIONS_PER_CLIENT] = {0};
  uint8_t state[SESSIONS_PER_CLIENT] = {0};
  uint32_t last_req_id[SESSIONS_PER_CLIENT];


  uint32_t stack_id[SESSIONS_PER_CLIENT] = {0};
  uint32_t stack_id_cntr = (uint32_t) machine_id * 100;
  int ret;

  for (s_i = 0; s_i < SESSIONS_PER_CLIENT; s_i++) {
    uint16_t real_sess_i = sess_offset + s_i;
    if (ENABLE_ASSERTIONS) assert(real_sess_i < SESSIONS_PER_MACHINE);
    stack_id[s_i] = stack_id_cntr;
    MOD_INCR(stack_id_cntr, NUM_OF_RMW_KEYS);
    last_req_id[s_i] = (uint32_t)  async_read_strong(stack_id[s_i], (uint8_t *) &top[s_i],
                                                    sizeof(struct top), real_sess_i);
    state[s_i] = TR_READ_TOP;
  }
  s_i = 0;
  while(true) {
    uint16_t real_sess_i = sess_offset + s_i;
    switch (state[s_i]) {
      case TR_READ_TOP:
        poll_a_req_blocking(real_sess_i, last_req_id[s_i]);
        if (top[s_i].key_id == 0) {
          //printf("poping nothing \n");
          success[s_i] = true;
        }
        else {
          if (top[s_i].key_id < NUM_OF_RMW_KEYS) {
            printf("Will pop_something %u, stack_id %u \n", top[s_i].key_id, stack_id[s_i]);
            assert(false);
          }
        }
        if (success[s_i]) {
          stack_id[s_i] = stack_id_cntr;
          MOD_INCR(stack_id_cntr, NUM_OF_RMW_KEYS);
          last_req_id[s_i] = (uint32_t) async_read_strong(stack_id[s_i], (uint8_t *) &top[s_i],
                                                          sizeof(struct top), real_sess_i);
          success[s_i] = false;
          state[s_i] = TR_READ_TOP;
          c_stats[t_id].microbench_pops++;
        }
        else {
          assert(top[s_i].key_id > 0);
          if (ENABLE_ASSERTIONS) assert(top[s_i].key_id >= NUM_OF_RMW_KEYS);
          for (uint16_t i = 0; i < TREIBER_WRITES_NUM; i++) {
            last_req_id[s_i] =
              (uint32_t) async_read_strong(top[s_i].key_id + i, (uint8_t *) &first_node[s_i],
                                           sizeof(struct node), real_sess_i);
          }
          state[s_i] = TR_READ_FIRST;
        }
        break;
      case TR_READ_FIRST:
        poll_a_req_blocking(real_sess_i, last_req_id[s_i]);
        new_top[s_i].pop_counter = top[s_i].pop_counter + 1;
        new_top[s_i].key_id = first_node[s_i].next_key_id;
        last_req_id[s_i] =
          (uint32_t) async_cas_strong(stack_id[s_i], (uint8_t *) &top[s_i], (uint8_t *) &new_top[s_i],
                                      sizeof(struct top), &success[s_i], true, real_sess_i);
        state[s_i] = TR_READ_TOP;
        break;
      default: assert(false);
    }
    MOD_INCR(s_i, SESSIONS_PER_CLIENT);
  }

  //printf("Completed %u pushes in %u stacks \n", SESSIONS_PER_MACHINE, SESSIONS_PER_MACHINE);
}


static inline void treiber_push_multi_session(uint16_t t_id)
{

  assert(sizeof(struct top) <= VALUE_SIZE);
  assert(sizeof(struct node) == VALUE_SIZE);
  uint16_t s_i = 0;
  uint32_t key_offset = (uint32_t) (NUM_OF_RMW_KEYS + (NUM_OF_RMW_KEYS * machine_id));
  uint16_t sess_offset = (uint16_t) (t_id * SESSIONS_PER_CLIENT);
  assert(sess_offset + SESSIONS_PER_CLIENT <= SESSIONS_PER_MACHINE);
  //uint32_t stack_ids[SESSIONS_PER_MACHINE];
  struct top top[SESSIONS_PER_CLIENT], new_top[SESSIONS_PER_CLIENT];
  struct node new_node[SESSIONS_PER_CLIENT][TREIBER_WRITES_NUM];
  bool success[SESSIONS_PER_CLIENT] = {0};
  uint32_t last_req_id[SESSIONS_PER_CLIENT];
  uint8_t state[SESSIONS_PER_CLIENT] = {0};
  uint32_t stack_id[SESSIONS_PER_CLIENT] = {0};
  uint32_t stack_id_cntr = (uint32_t) machine_id * 100;
  //my_printf(green, "Read top_key_id %u, points to key %u top counter %u\n",
  //              stack_id, top.key_id, top.counter);
  //uint32_t success_cntr = 0;
  for (s_i = 0; s_i < SESSIONS_PER_CLIENT; s_i++) {
    uint16_t real_sess_i = sess_offset + s_i;
    if (ENABLE_ASSERTIONS) assert(real_sess_i < SESSIONS_PER_MACHINE);
    stack_id[s_i] = stack_id_cntr;
    MOD_INCR(stack_id_cntr, NUM_OF_RMW_KEYS);
    last_req_id[s_i] = (uint32_t) async_read_strong(stack_id[s_i], (uint8_t *) &top[s_i],
                                                    sizeof(struct top), real_sess_i);
    for (uint16_t i = 1; i < TREIBER_WRITES_NUM; i++) {
      uint32_t key_to_write = key_offset + (s_i * TREIBER_WRITES_NUM) + i;
      async_write_strong(key_to_write, (uint8_t *) &new_node[s_i][i],
                         sizeof(struct node), real_sess_i);
    }
    state[s_i] = TR_READ_TOP;
  }
  s_i = 0;
  while(true) {
    uint16_t real_sess_i = sess_offset + s_i;
    switch (state[s_i]) {
      case TR_READ_TOP:
        if (ENABLE_ASSERTIONS) assert(real_sess_i < SESSIONS_PER_MACHINE);
        poll_a_req_blocking(real_sess_i, last_req_id[s_i]);
        if (top[s_i].key_id != 0 && top[s_i].key_id < NUM_OF_RMW_KEYS) {
          printf("Stack id %u points to key %u \n", stack_id[s_i], top[s_i].key_id);
          assert(false);
        }
        if (success[s_i]) {
          stack_id[s_i] = stack_id_cntr;
          MOD_INCR(stack_id_cntr, NUM_OF_RMW_KEYS);
          last_req_id[s_i] = (uint32_t) async_read_strong(stack_id[s_i], (uint8_t *) &top[s_i],
                                                          sizeof(struct top), real_sess_i);
          for (uint16_t i = 1; i < TREIBER_WRITES_NUM; i++) {
            uint32_t key_to_write = key_offset + (s_i * TREIBER_WRITES_NUM) + i;
            async_write_strong(key_to_write, (uint8_t *) &new_node[s_i][i],
                               sizeof(struct node), real_sess_i);
          }
          success[s_i] = false;
          c_stats[t_id].microbench_pushes++;
          break;
        }

        // Do only one write here: the one that needs to point to what top used to point
        // the rest of the writes need not happen in the conflict path, and need not wait
        // for the previous read to compelte
        new_node[s_i][0].next_key_id = top[s_i].key_id;
        uint32_t key_to_write = key_offset + s_i;
        async_write_strong(key_to_write, (uint8_t *) &new_node[s_i][0],
                           sizeof(struct node), real_sess_i);


        new_top[s_i].pop_counter = top[s_i].pop_counter;
        new_top[s_i].key_id = key_to_write;
        assert(new_top[s_i].key_id >= NUM_OF_RMW_KEYS);
        last_req_id[s_i] = (uint32_t) async_cas_strong(stack_id[s_i], (uint8_t *) &top[s_i], (uint8_t *) &new_top[s_i],
                                                       sizeof(struct top), &success[s_i], true, real_sess_i);
        break;
      default: assert(false);
    }
    MOD_INCR(s_i, SESSIONS_PER_CLIENT);
  }

}

static inline void treiber_pop_state_machine_dbg(struct tr_sess_info_dbg *info,
                                             uint32_t* stack_id_cntr,
                                             uint16_t t_id)
{
  struct top *top = &info->pop_top;
  struct top *new_top = &info->pop_new_top;
  struct node *first_node = info->new_node;
  uint16_t real_sess_i = info->real_sess_i;
  uint16_t s_i = info->s_i;
  if (ENABLE_ASSERTIONS) assert(real_sess_i < SESSIONS_PER_MACHINE);
  uint32_t key_to_write;
  //printf("Session %u %p %p \n", s_i, info, top);

  //my_printf(green, "Session %u state %u, top key_id %u\n", s_i, info->state, top->key_id);
  switch (info->state) {
    case TR_INIT:
      assert(!info->success);
      info->last_req_id = (uint32_t)  async_read_strong(info->stack_id, (uint8_t *) top,
                                                       sizeof(struct top), real_sess_i);
      info->state = TR_READ_TOP;
      break;
    case TR_READ_TOP:
      poll_a_req_blocking(real_sess_i, info->last_req_id);
//      if (!info->done_cas) my_printf(magenta, "Pop read top key_id %u, push/pop %u/%u \n",
//                                     top->key_id, top->push_counter, top->pop_counter);
      check_top(top, "Pop-after reading top ", info->stack_id);
      if (top->key_id == 0) {
        printf("Popping from empty stack %u success %d push counter/pop counter %u/%u read from %s\n",
               info->stack_id, info->success, top->push_counter, top->pop_counter, info->done_cas ? "cas" : "read");
        info->success = true;
        assert(false);
      }
      if (!info->success) { //either CAS failed, or has not been attempted yet
        if (ENABLE_ASSERTIONS) assert(top->key_id >= TR_KEY_OFFSET);
        assert(top->key_id <= MAX_TR_NODE_KEY);
        for (uint16_t i = 0; i < TREIBER_WRITES_NUM; i++) {
          info->last_req_id =
            //(uint32_t) async_acquire_strong(top->key_id + i, (uint8_t *) &first_node[i],
            //                                sizeof(struct node), real_sess_i);
            (uint32_t) async_read_strong(top->key_id + i, (uint8_t *) &first_node[i],
                                         sizeof(struct node), real_sess_i);
        }
        info->state = TR_READ_FIRST;
      }
      else if (info->success) {
        //assert(first_node[0].pushed);
        uint32_t dbg_count = 0;
        while  (first_node[0].stack_id != (uint16_t) info->stack_id) {
//          if (dbg_count == 0) {
//            struct key key = create_key(info->stack_id);
//            my_printf(yellow, "Sess %u/%u,  Popping stack %u -- Read node %u stack id %u, pushed %u, owner %u,"
//                            "bkt %u  key id %u push/pop %u/%u \n",
//                          info->glob_sess_i, t_id, info->stack_id, top->key_id,
//                          first_node[0].stack_id, first_node[0].pushed, first_node[0].owner, key.bkt,
//            new_top->key_id, new_top->push_counter, new_top->pop_counter);
//          }
          blocking_read(top->key_id, (uint8_t *) &first_node[0],
                            sizeof(struct node), real_sess_i);
          MOD_INCR(dbg_count, M_4);
//          if (first_node[0].stack_id == (uint16_t) info->stack_id)
//            my_printf(green, "Repeat: Sess %u Popping stack %u -- Read node %u stack id %u \n",
//                        info->glob_sess_i,
//                        info->stack_id, top->key_id, first_node[0].stack_id);
          //assert(false);
        }

        if (top->sec_key_id != 1) {
          while (top->sec_key_id != first_node[0].next_key_id) {
//            struct key key = create_key(info->stack_id);
//            if (dbg_count == 0)
//              printf("sess %u/%u, after popping stack %u/%u (bkt %u ), "
//                       "old top %u/%u/%u, new_top %u/%u/%u"
//                       " got key %u/%u "
//                       "left first node: key %u/%d/%u"
//                       " -- it should have been  %u/%d/%u \n",
//                info->glob_sess_i, t_id, info->stack_id, first_node[0].stack_id, key.bkt,
//                     top->key_id, top->push_counter, top->pop_counter,
//                     new_top->key_id, new_top->push_counter, new_top->pop_counter,
//                     top->key_id, create_key(top->key_id).bkt,
//                     first_node[0].next_key_id, first_node[0].pushed, first_node[0].push_counter,
//                     top->sec_key_id, true, top->push_counter);
            blocking_read(top->key_id, (uint8_t *) &first_node[0],
                          sizeof(struct node), real_sess_i);
//            if (top->sec_key_id == first_node[0].next_key_id)
//              my_printf(green, "sess %u/%u, Read key becomes correct %u/%u/%u/%u \n",
//                           info->glob_sess_i, t_id, first_node[0].next_key_id,
//                           first_node[0].pushed, first_node[0].stack_id, first_node[0].push_counter);
            MOD_INCR(dbg_count, M_4);
          }
        }

        info->owned_key = top->key_id;
        if (TREIBER_WRITES_NUM == 2) assert(info->owned_key % 2 == 0);
        first_node[0].pushed = false;
        first_node[0].stack_id = (uint16_t) (NUMBER_OF_STACKS + info->stack_id);
        first_node[0].owner = info->glob_sess_i;
        async_write_strong(info->owned_key, (uint8_t *) &first_node[0],
                           sizeof(struct node), real_sess_i);


        check_top(new_top, "Pop-new_top on success ", info->stack_id);
//        my_printf(cyan, "Session %u Popped key %u from stack %u pushed/pulled %u/%u \n",
//                    info->real_sess_i, top->key_id, info->stack_id, new_top->push_counter, new_top->pop_counter);
        update_file(t_id, top->key_id, info, false, top, new_top);
        info->success = false;
        info->done_cas = false;
        if (!TREIBER_NO_CONFLICTS) {
          info->stack_id = *stack_id_cntr;
          MOD_INCR(*stack_id_cntr,
                  NUMBER_OF_STACKS);// (top->key_id + new_top->push_counter + (uint32_t)(&info->stack_id) +
        }
          //(uint32_t)(c_stats[t_id].treiber_pops * c_stats[t_id].treiber_pushes) ) % NUMBER_OF_STACKS;
        c_stats[t_id].microbench_pops++;

        info->valid_key_to_write = true;
        // Transition to pushing
        info->push_or_pull_state = PUSHING;
        info->state = TR_INIT;
      }
      break;
    case TR_READ_FIRST:
      poll_a_req_blocking(real_sess_i, info->last_req_id);
      new_top->pop_counter = top->pop_counter + 1;
      new_top->push_counter = top->push_counter;
      new_top->key_id = first_node[0].next_key_id;
      new_top->sec_key_id = top->third_key_id;
      new_top->third_key_id = top->fourth_key_id; // we concede that we do not know that
      new_top->fourth_key_id = 1;
      //assert(first_node[0].stack_id = (uint16_t) info->stack_id);
      //assert(first_node[0].push_counter <= top->push_counter);


      check_top(top, "Pop-already checked ", info->stack_id);
       // Below usage of check_top is okay to fire -- it means we read a stale first node
      // (i.e. the first_node[0].next_key_id) may be off and that is okay
      if (!check_top(new_top, "Pop-new_top before CAS ", info->stack_id)) {
        //my_printf(cyan, "Before popping stack %u, new_top key/top key %u/%u, new pushed/pulled %u/%u\n",
        //            info->stack_id, new_top->key_id, top->key_id, new_top->push_counter, new_top->pop_counter);
        if (info->pop_dbg <= DEBUG_MAX) info->pop_dbg++;
        if (info->pop_dbg == DEBUG_MAX) {
          printf("Sess %u stack when popping for stack %u, push/pop %u/%u next key id %u, current key_id %u \n",
                 info->real_sess_i, info->stack_id, new_top->push_counter,
                 new_top->pop_counter, new_top->key_id, top->key_id);
          //info->pop_dbg = 0;
        }
        info->state = TR_INIT;
        break;
        // TOP MUST HAVE BEEN UPDATED FOR THAT KEY TO BE ZERO
      }
      if (info->pop_dbg >= DEBUG_MAX) {
        my_printf(green, "Sess %u Unstack when popping for stack %u, push/pop %u/%u next key id %u, current key_id %u \n",
                     info->real_sess_i, info->stack_id, new_top->push_counter,
                     new_top->pop_counter, new_top->key_id, top->key_id);
      }
      info->pop_dbg = 0;
      //check_top(new_top, "Pop-new_top before CAS ", info->stack_id);
      info->last_req_id =
        (uint32_t) async_cas_strong(info->stack_id, (uint8_t *) top, (uint8_t *) new_top,
                                    sizeof(struct top), &info->success, true, real_sess_i);
      info->done_cas = true;
      info->state = TR_READ_TOP;
      break;
    default: assert(false);
  }
}


static inline void treiber_push_state_machine_dbg(struct tr_sess_info_dbg *info,
                                              uint32_t* stack_id_cntr, uint16_t t_id)
{
  struct top *top = &info->top;
  struct top *new_top = &info->new_top;
  struct node *new_node = info->new_node;
  uint16_t real_sess_i = info->real_sess_i;
  uint16_t s_i = info->s_i;
  if (ENABLE_ASSERTIONS) assert(real_sess_i < SESSIONS_PER_MACHINE);
  //uint32_t key_to_write;
  //printf("Session %u %p %p \n", s_i, info, top);

  //my_printf(green, "Session %u state %u, top key_id %u\n", s_i, info->state, top->key_id);
  switch (info->state) {
    case TR_INIT: // INITIAL phase of a push: come here in the first push ever, and after every successful push
      //MOD_INCR(*stack_id_cntr, NUMBER_OF_STACKS);
      info->done_cas = false;
      assert(!info->success);
      async_read_strong(info->owned_key, (uint8_t *) info->owned_node,
                        sizeof(struct node), real_sess_i);
      info->last_req_id = (uint32_t) async_read_strong(info->stack_id, (uint8_t *) top,
                                                       sizeof(struct top), real_sess_i);

      //if (ENABLE_ASSERTIONS)
      //  t_stats[info->wrkr].writes_asked_by_clients+= (TREIBER_WRITES_NUM - 1);

//      if (info->wrkr == 13)
//        my_printf(yellow, "Clt %u have Asked worker %u for %lu writes \n",
//                      t_id, info->wrkr, t_stats[info->wrkr].writes_asked_by_clients);
      for (uint16_t i = 1; i < TREIBER_WRITES_NUM; i++) {
        //assert(false);

        async_write_strong(info->owned_key + i, (uint8_t *) &new_node[i],
                           sizeof(struct node), real_sess_i);
      }
      info->state = TR_READ_TOP;
      break;
    case TR_READ_TOP:
      poll_a_req_blocking(real_sess_i, info->last_req_id);
      if (!info->done_cas) {
        if (memcmp(info->owned_node, &new_node[0], sizeof(struct node)) != 0) {
          printf("Sess %u, On pushing checking owned key %u, stack_ids %u/%u "
                   "owned/new key id %u/%u, owner %u/%u/ pushed %u/%u \n", info->glob_sess_i,
                 info->owned_key, info->owned_node->stack_id, new_node[0].stack_id,
                 info->owned_node->next_key_id, new_node[0].next_key_id,
                  info->owned_node->owner, new_node[0].owner,
                 info->owned_node->pushed, new_node[0].pushed);
          //assert(false);
        }
      }

      if (!info->success) {
        assert(check_top(top, "Push-after reading top ", info->stack_id));
        // Do only one write here: the one that needs to point to what top used to point
        // the rest of the writes need not happen in the conflict path, and need not wait
        // for the previous read to complete
        if (info->done_cas) {
          uint32_t dbg_count = 0;
          while (new_top->push_counter - 1 == top->push_counter &&
            new_top->pop_counter == top->pop_counter) {
            if (dbg_count == 0) {
              my_printf(red, "Sess %u/%u Stack_id %u, CAS failed, top %u/%u/%u, prev top %u/%u/%u \n",
            info->glob_sess_i, t_id, info->stack_id, top->key_id, top->push_counter, top->pop_counter,
                       new_node[0].next_key_id, new_top->push_counter - 1, new_top->pop_counter);
            }

            blocking_read(info->stack_id, (uint8_t *) top,
                          sizeof(struct top), real_sess_i);

            if (dbg_count == 0) {
              my_printf(red, "Sess %u/%u Stack_id %u, CAS failed-blocking read, top %u/%u/%u, prev top %u/%u/%u \n",
                         info->glob_sess_i, t_id, info->stack_id, top->key_id, top->push_counter, top->pop_counter,
                         new_node[0].next_key_id, new_top->push_counter - 1, new_top->pop_counter);
            }

            MOD_INCR(dbg_count, M_4);
          }
        }

        new_node[0].next_key_id = top->key_id;
        new_node[0].owner = info->glob_sess_i;
        new_node[0].stack_id = (uint16_t) info->stack_id;
        new_node[0].push_counter = top->push_counter + 1;
        new_node[0].pushed = true;
        new_node[0].key_id = info->owned_key;

        assert(info->valid_key_to_write);
        new_top->pop_counter = top->pop_counter;
        new_top->push_counter = top->push_counter + 1;
        new_top->key_id = info->owned_key;
        new_top->sec_key_id = top->key_id;
        new_top->third_key_id = top->sec_key_id;
        new_top->fourth_key_id = top->third_key_id;
        assert(new_top->key_id >= TR_KEY_OFFSET);
        assert(info->owned_key  <= MAX_TR_NODE_KEY);
        if (TREIBER_WRITES_NUM == 2) assert(info->owned_key % 2 == 0);

        check_node((uint8_t *)&new_node[0], "client before pushing", info->stack_id);
//        printf("Sess %u Trying to push %u  to stack %u : %u/%u \n",
//               real_sess_i, info->owned_key, info->stack_id, new_top->push_counter, new_top->pop_counter);

        //if (ENABLE_ASSERTIONS) t_stats[info->wrkr].writes_asked_by_clients++;

        async_write_strong(info->owned_key, (uint8_t *) &new_node[0],
                           sizeof(struct node), real_sess_i);
        //async_release_strong(MAX_TR_NODE_KEY + info->glob_sess_i, (uint8_t *) &new_node[0],
         //                    sizeof(struct node), real_sess_i);
        info->last_req_id = (uint32_t) async_cas_strong(info->stack_id, (uint8_t *) top, (uint8_t *) new_top,
                                                        sizeof(struct top), &info->success, true, real_sess_i);
        assert(info->last_req_id > 0);
        info->done_cas = true;
      }
      else if (info->success) { // If push succeeded, do a pull
//        my_printf(yellow, "Session %u Pushed key %u to stack %u : %u/%u \n",
//                      info->real_sess_i, new_top->key_id, info->stack_id,
//                      new_top->push_counter, new_top->pop_counter);
        info->valid_key_to_write = false;
        check_top(new_top, "Push-new_top after success ", info->stack_id);
        //update_file(t_id, info->key_to_write, info, true, top, new_top);
        info->done_cas = false;
        info->success = false;
        c_stats[t_id].microbench_pushes++;
        assert(c_stats[t_id].microbench_pushes > c_stats[t_id].microbench_pops);
        info->push_or_pull_state = POPPING;
        info->state = TR_INIT;
      }
      break;
    default: assert(false);
  }
}


// Each session picks a stack, pushes one element and then pops one element
static inline void treiber_push_pull_multi_session_dbg(uint16_t t_id)
{

  assert(sizeof(struct top) <= VALUE_SIZE);
  assert(sizeof(struct node) == VALUE_SIZE);
  assert(TREIBER_WRITES_NUM > 0);
  uint16_t s_i = 0;
  uint32_t key_offset = (uint32_t) (TR_KEY_OFFSET + (SESSIONS_PER_MACHINE * machine_id));
  uint16_t sess_offset = (uint16_t) (t_id * SESSIONS_PER_CLIENT);
  assert(sess_offset + SESSIONS_PER_CLIENT <= SESSIONS_PER_MACHINE);
  uint32_t stack_id_cntr = 0;//(uint32_t) machine_id * 100;
  struct tr_sess_info_dbg *info = calloc(SESSIONS_PER_CLIENT, sizeof(struct tr_sess_info_dbg));
  uint32_t first_key = key_offset + sess_offset * TREIBER_WRITES_NUM;
  uint32_t last_key = key_offset + (sess_offset + (SESSIONS_PER_CLIENT -1)) * TREIBER_WRITES_NUM;
  uint16_t min_sess = sess_offset, max_sess = (uint16_t) (sess_offset + (SESSIONS_PER_CLIENT -1));
  uint16_t min_wrkr = (uint16_t) (min_sess / SESSIONS_PER_THREAD),
    max_wrkr = (uint16_t) (max_sess / SESSIONS_PER_THREAD);
  printf("Client %u: sessions %u to %u, workers %u to %u, keys %u to %u, globally max key %u \n",
         t_id, min_sess, max_sess, min_wrkr, max_wrkr,
         first_key, last_key, MAX_TR_NODE_KEY);

  for (s_i = 0; s_i < SESSIONS_PER_CLIENT; s_i++) {
    info[s_i].state = TR_INIT;
    info[s_i].push_or_pull_state = PUSHING;
    info[s_i].s_i = s_i;
    info[s_i].real_sess_i = sess_offset + s_i;
    info[s_i].glob_sess_i = (uint16_t) (SESSIONS_PER_MACHINE * machine_id + info[s_i].real_sess_i);
    info[s_i].owned_key = (uint32_t) (TR_KEY_OFFSET + (info[s_i].glob_sess_i * TREIBER_WRITES_NUM));
    //printf("owned %u \n", info[s_i].owned_key);
    if (TREIBER_WRITES_NUM == 2) assert(info->owned_key % 2 == 0);

    //printf("Session %u key %u \n", info[s_i].real_sess_i, info[s_i].key_to_write);
    if (ENABLE_ASSERTIONS) assert(info[s_i].real_sess_i < SESSIONS_PER_MACHINE);
    info[s_i].new_node = calloc(TREIBER_WRITES_NUM, (sizeof(struct node)));
    info[s_i].new_node[0].next_key_id = 0;
    info[s_i].stack_id =  (key_offset + info[s_i].real_sess_i - TR_KEY_OFFSET) % NUMBER_OF_STACKS;
    assert(info[s_i].stack_id < NUMBER_OF_STACKS);
    info[s_i].valid_key_to_write = true;
    info[s_i].wrkr = (uint16_t) (info[s_i].real_sess_i / SESSIONS_PER_THREAD);
    info[s_i].owned_node = calloc(1,(sizeof(struct node)));
  }
  s_i = 0;
  while(true) {
    switch (info[s_i].push_or_pull_state) {
      case PUSHING:
        treiber_push_state_machine_dbg(&info[s_i], &stack_id_cntr, t_id);
        break;
      case POPPING:
        treiber_pop_state_machine_dbg(&info[s_i], &stack_id_cntr, t_id);
        break;
      default: if (ENABLE_ASSERTIONS) assert(false);
    }
    MOD_INCR(s_i, SESSIONS_PER_CLIENT);
  }

}

struct tr_sess_info {
  uint8_t state;
  uint8_t push_or_pull_state;
  bool success;
  //bool valid_key_to_write;
  //bool done_cas;
  uint16_t s_i;
  uint16_t real_sess_i;
  //uint16_t glob_sess_i;
  uint16_t wrkr;
  uint32_t last_req_id;
  uint32_t stack_id;
  uint32_t owned_key;
  uint32_t pop_dbg;
  //uint32_t push_dbg;
  struct top top;
  struct top new_top;
  struct node *new_node;
  uint32_t last_pushed_counter; // debug
  //struct node *owned_node;
};

static inline void treiber_pop_state_machine(struct tr_sess_info *info,
                                             uint32_t *stack_id_cntr,
                                             uint16_t t_id)
{
  struct top *top = &info->top;
  struct top *new_top = &info->new_top;
  struct node *first_node = info->new_node;
  uint16_t real_sess_i = info->real_sess_i;
  if (ENABLE_ASSERTIONS) assert(real_sess_i < SESSIONS_PER_MACHINE);
  switch (info->state) {
    case TR_INIT:

      if (ENABLE_ASSERTIONS) assert(!info->success);
      info->last_req_id = (uint32_t) async_read_strong(info->stack_id, (uint8_t *) top,
                                                       sizeof(struct top), real_sess_i);
      info->state = TR_READ_TOP;
      break;
    case TR_READ_TOP:

      poll_a_req_blocking(real_sess_i, info->last_req_id);
      if (top->key_id < TR_KEY_OFFSET ||info->last_pushed_counter > top->push_counter ) {
        my_printf(red, "POPPING: success %d, top->key.id %u, push/pop %u/%u last pushed %u\n",
                  info->success, top->key_id, top->push_counter, top->pop_counter, info->last_pushed_counter);
        assert(false);
      }
      if (!info->success) { //either CAS failed, or has not been attempted yet
        check_top(top, "", info->stack_id);
        for (uint16_t i = 0; i < TREIBER_WRITES_NUM; i++) {
          info->last_req_id =
            (uint32_t) async_read_strong(top->key_id + i, (uint8_t *) &first_node[i],
                                         sizeof(struct node), real_sess_i);
        }
        info->state = TR_READ_FIRST;
      }
      else if (info->success) {
        info->owned_key = top->key_id;
        assert(top->key_id >= TR_KEY_OFFSET);
        // my_printf(cyan, "Session %u Popped key %u from stack %u pushed/pulled %u/%u \n",
        //            info->real_sess_i, top->key_id, info->stack_id, new_top->push_counter, new_top->pop_counter);
        info->success = false;
        if (!TREIBER_NO_CONFLICTS) {
           info->stack_id = *stack_id_cntr;
           MOD_INCR(*stack_id_cntr, NUMBER_OF_STACKS);
        }
        c_stats[t_id].microbench_pops++;
        // Transition to pushing
        info->push_or_pull_state = PUSHING;
        info->state = TR_INIT;
      }
      break;
    case TR_READ_FIRST:
      poll_a_req_blocking(real_sess_i, info->last_req_id);
      new_top->pop_counter = top->pop_counter + 1;
      new_top->push_counter = top->push_counter;
      new_top->key_id = first_node[0].next_key_id;
      if ((new_top->push_counter == new_top->pop_counter && new_top->key_id != 0) ||
           new_top->push_counter >  new_top->pop_counter && new_top->key_id == 0) {
        if (CLIENT_ASSERTIONS) {
          info->pop_dbg++;
          if (info->pop_dbg == DEBUG_MAX)
            my_printf(red, "A session is stuck %u \n", real_sess_i);
        }
        info->state = TR_INIT; break;
      }
      if (CLIENT_ASSERTIONS) {
        if (info->pop_dbg >= DEBUG_MAX)
          my_printf(green, "Session %u unstuck \n", real_sess_i);
        info->pop_dbg = 0;
      }


      check_top(top, "Pop-already checked ", info->stack_id);
      check_top(new_top, "Pop-new_top ", info->stack_id);
      //assert(new_top->key_id == 0);
      // my_printf(green, "Sess %u attempting to pop stack %u \n", info->real_sess_i, info->stack_id);
      info->last_req_id =
        (uint32_t) async_cas_strong(info->stack_id, (uint8_t *) top, (uint8_t *) new_top,
                                    sizeof(struct top), &info->success, true, real_sess_i);
      info->state = TR_READ_TOP;
      break;
    default: if (ENABLE_ASSERTIONS) assert(false);
  }
}


static inline void treiber_push_state_machine(struct tr_sess_info *info,
                                             uint16_t t_id)
{
  struct top *top = &info->top;
  struct top *new_top = &info->new_top;
  struct node *new_node = info->new_node;
  uint16_t real_sess_i = info->real_sess_i;

  if (ENABLE_ASSERTIONS) assert(real_sess_i < SESSIONS_PER_MACHINE);

  switch (info->state) {
    case TR_INIT: // INITIAL phase of a push: come here in the first push ever, and after every successful push
      if (ENABLE_ASSERTIONS) assert(!info->success);
      info->last_req_id = (uint32_t) async_read_strong(info->stack_id, (uint8_t *) top,
                                                       sizeof(struct top), real_sess_i);
      for (uint16_t i = 1; i < TREIBER_WRITES_NUM; i++) {
         async_write_strong(info->owned_key + i, (uint8_t *) &new_node[i],
                           sizeof(struct node), real_sess_i);
      }
      info->state = TR_READ_TOP;
      break;
    case TR_READ_TOP:
      // my_printf(red,"Sess %u starts polling\n", info->real_sess_i);
      poll_a_req_blocking(real_sess_i, info->last_req_id);
      // my_printf(yellow,"Sess %u finishes polling\n", info->real_sess_i);
      assert(check_top(top, "Push-after reading top ", info->stack_id));
      if (!info->success) {
        // Do only one write here: the one that needs to point to what top used to point
        // the rest of the writes need not happen in the conflict path, and need not wait
        // for the previous read to complete
        new_node[0].next_key_id = top->key_id;
        new_top->pop_counter = top->pop_counter;
        new_top->push_counter = top->push_counter + 1;
        new_top->key_id = info->owned_key;

        if (ENABLE_ASSERTIONS) {
          //if (new_top->pop_counter == new_top->push_counter) assert(new_top->key_id == 0);
          assert(new_top->key_id >= TR_KEY_OFFSET);
          assert(info->owned_key <= MAX_TR_NODE_KEY);
        }
        check_node((uint8_t *)&new_node[0], "client before pushing", info->stack_id);
        assert(check_top(new_top, "Push-before writing new_top ", info->stack_id));
        async_write_strong(info->owned_key, (uint8_t *) &new_node[0],
                           sizeof(struct node), real_sess_i);

        if (new_top->pop_counter == new_top->push_counter) assert(new_top->key_id == 0);
        info->last_req_id = (uint32_t) async_cas_strong(info->stack_id, (uint8_t *) top, (uint8_t *) new_top,
                                                        sizeof(struct top), &info->success, true, real_sess_i);
      }
      else if (info->success) { // If push succeeded, do a pull
        // my_printf(yellow, "Session %u Pushed key %u to stack %u : %u/%u \n",
        //              info->real_sess_i, new_top->key_id, info->stack_id,
        //              new_top->push_counter, new_top->pop_counter);
        check_top(new_top, "Push-new_top after success ", info->stack_id);
        info->success = false;
        c_stats[t_id].microbench_pushes++;
        if (ENABLE_ASSERTIONS) {
          assert(c_stats[t_id].microbench_pushes > c_stats[t_id].microbench_pops);
        }
        info->push_or_pull_state = POPPING;

        info->state = TR_INIT;
        info->last_pushed_counter = new_top->push_counter;
      }
      break;
    default: if (ENABLE_ASSERTIONS) assert(false);
  }
}


// Each session picks a stack, pushes one element and then pops one element
static inline void treiber_push_pull_multi_session(uint16_t t_id)
{

  assert(sizeof(struct top) <= VALUE_SIZE);
  assert(sizeof(struct node) == VALUE_SIZE);
  assert(TREIBER_WRITES_NUM > 0);
  assert(NUMBER_OF_STACKS >= GLOBAL_SESSION_NUM);
  uint16_t s_i = 0;
  uint32_t key_offset = (uint32_t) (TR_KEY_OFFSET + (SESSIONS_PER_MACHINE * machine_id));
  uint16_t sess_offset = (uint16_t) (t_id * SESSIONS_PER_CLIENT);
  assert(sess_offset + SESSIONS_PER_CLIENT <= SESSIONS_PER_MACHINE);
  struct tr_sess_info *info = calloc(SESSIONS_PER_CLIENT, sizeof(struct tr_sess_info));
  uint32_t first_key = key_offset + sess_offset * TREIBER_WRITES_NUM;
  uint32_t last_key = key_offset + (sess_offset + (SESSIONS_PER_CLIENT -1)) * TREIBER_WRITES_NUM;
  uint16_t min_sess = sess_offset, max_sess = (uint16_t) (sess_offset + (SESSIONS_PER_CLIENT -1));
  uint16_t min_wrkr = (uint16_t) (min_sess / SESSIONS_PER_THREAD),
    max_wrkr = (uint16_t) (max_sess / SESSIONS_PER_THREAD);

  uint32_t stack_id_cntr = ((machine_id * 100) + last_key) % NUMBER_OF_STACKS;
  printf("Client %u: sessions %u to %u, workers %u to %u, keys %u to %u, globally max key %u \n",
         t_id, min_sess, max_sess, min_wrkr, max_wrkr,
         first_key, last_key, MAX_TR_NODE_KEY);

  for (s_i = 0; s_i < SESSIONS_PER_CLIENT; s_i++) {
    info[s_i].state = TR_INIT;
    info[s_i].push_or_pull_state = PUSHING;
    info[s_i].s_i = s_i;
    info[s_i].real_sess_i = sess_offset + s_i;
    //info[s_i].owned_key = key_offset + info[s_i].real_sess_i * TREIBER_WRITES_NUM;
    info[s_i].owned_key = (uint32_t) (TR_KEY_OFFSET + (
                          (SESSIONS_PER_MACHINE * machine_id + info[s_i].real_sess_i) * TREIBER_WRITES_NUM));
    //printf("Session %u key %u \n", info[s_i].real_sess_i, info[s_i].key_to_write);
    assert(info[s_i].real_sess_i < SESSIONS_PER_MACHINE);
    info[s_i].new_node = calloc(TREIBER_WRITES_NUM, (sizeof(struct node)));
    info[s_i].new_node[0].next_key_id = 0;
    info[s_i].stack_id =  (uint16_t) (SESSIONS_PER_MACHINE * machine_id + info[s_i].real_sess_i);
    //(info[s_i].owned_key - KEY_OFFSET) % NUMBER_OF_STACKS;
    assert(info[s_i].stack_id < NUMBER_OF_STACKS);
  }
  s_i = 0;
  while(true) {
    switch (info[s_i].push_or_pull_state) {
      case PUSHING:
        treiber_push_state_machine(&info[s_i], t_id);
        break;
      case POPPING:
        treiber_pop_state_machine(&info[s_i], &stack_id_cntr, t_id);
        break;
      default: if (ENABLE_ASSERTIONS) assert(false);
    }
    MOD_INCR(s_i, SESSIONS_PER_CLIENT);
  }

}

/* ------------------------------------------------------------------------------------------------------------------- */
/*------------------------------M&S QUEUE------------------------------------------------------------------------------*/
/* ------------------------------------------------------------------------------------------------------------------- */


#define MS_ENQUEUING 0
#define MS_DEQUEUING 1

//MS_STATES
#define MS_INIT 0
#define MS_LOOP_START 1
#define MS_READ_TAIL 2
#define MS_READ_LAST_NODE 3
#define MS_POTENTIAL_SUCCESS 4

#define MS_READ_HEAD 2
#define MS_READ_FIRST_NODE 3

#define MS_NODE_SIZE (VALUE_SIZE - 8)
#define MS_PTR_SIZE 12;


// KEY ALLOCATION-- SIZES & OFFSETS
#define MS_QUEUES_NUM (GLOBAL_SESSION_NUM)
#define MS_NODE_NUM (GLOBAL_SESSION_NUM) // 1 per session is enough, as each session only owns one at a time
#define DUMMY_KEYS_NUM (MS_QUEUES_NUM)
// all dummies point to 0 when starting
// the dummy node has no body, it's just a ptr
// there is 1 Dummy per Queue

#define MS_TAIL_KEY_ID_OFFSET 0
#define MS_HEAD_KEY_ID_OFFSET MS_QUEUES_NUM
#define MS_NODE_PTR_OFFSET (2 * MS_QUEUES_NUM)
#define LAST_MS_NODE_PTR (MS_NODE_PTR_OFFSET + DUMMY_KEYS_NUM + MS_NODE_NUM)

#define DUMMY_KEY_ID_OFFSET (LAST_MS_NODE_PTR + 1)
#define MS_NODE_KEY_OFFSET (DUMMY_KEY_ID_OFFSET + (MS_WRITES_NUM * DUMMY_KEYS_NUM)) //after dummies
#define MS_INIT_DONE_FLAG_KEY (MS_NODE_KEY_OFFSET + (MS_WRITES_NUM * MS_NODE_NUM)) // after all nodes

#define MS_INIT_DONE_FLAG 162


/* ----Key allocation----
 * 0 -> MS_QUEUES_NUM                 : tails
 * MS_QUEUES_NUM   -> 2*MS_QUEUES_NUM : heads
 * 2*MS_QUEUES_NUM -> 2*MS_QUEUES_NUM + DUMMY_KEYS_NUM + MS_NODE_NUM   : pointers of nodes
 * ---NON-RMWABLE
 * DUMMY_KEY_ID_OFFSET -> DUMMY_KEY_ID_OFFSET + DUMMY_KEYS_NUM : dummy nodes (1 per queue)
 * DUMMY_KEY_ID_OFFSET + DUMMY_KEYS_NUM -> ++ MS_NODE_NUM * MS_WRITES_NUM : bodies of nodes
 * MS_NODE_KEY_OFFSET + (MS_WRITES_NUM * MS_NODE_NUM) : init_done flag (INIT_DONE_FLAG_KEY)
 *
 * */

struct ms_ptr {
  bool pushed;
  uint32_t queue_id;
  uint32_t my_key_id;
  uint32_t next_key_id;
  uint64_t counter;
};
struct ms_node {
  uint8_t value[MS_NODE_SIZE];
  uint32_t node_id;
  uint32_t node_ptr_key_id;
};


struct ms_sess_info {
  uint8_t state;
  uint8_t enq_or_deq_state;
  bool success;
  bool advance_tail_result;
  bool tail_left_behind;
  //bool valid_key_to_write;
  //bool done_cas;
  uint16_t s_i;
  uint16_t real_sess_i;
  uint16_t glob_sess_i;
  uint16_t wrkr;
  uint32_t last_req_id;
  uint32_t queue_id;
  uint32_t owned_key; // they key of the owned ms_node
  uint32_t owned_key_ptr;
  uint32_t enqueue_num;
  uint32_t dequeue_num;
  //uint32_t pop_dbg;
  //uint32_t push_dbg;
  struct ms_ptr tail;
  struct ms_ptr second_tail;
  struct ms_ptr new_tail;
  struct ms_ptr head;
  struct ms_ptr second_head;
  struct ms_ptr new_head;
  struct ms_node *new_node;
  struct ms_node *last_or_first_node;
  struct ms_ptr owned_node_ptr; // even though there are many new nodes, there is only one pointer for them
  struct ms_ptr last_or_first_node_ms_ptr;
  struct ms_ptr new_last_or_first_node_ms_ptr;
  //struct node *owned_node;
};

static inline void print_ms_ptr(struct ms_ptr *ptr)
{
  my_printf(yellow, "-----------MS_PTR-%u----------\n", ptr->my_key_id);
  my_printf(yellow, "Queue id %u \n", ptr->queue_id);
  my_printf(yellow, "Next key-id %u \n", ptr->next_key_id);
  my_printf(yellow, "Counter %u \n", ptr->counter);
  my_printf(yellow, "Pushed: %s \n", ptr->pushed ? "YES": "NO");
}


static inline void check_write_if_msq_active(mica_op_t *kv_ptr, uint8_t *new_val,
                                             uint8_t flag)
{
  if (!ENABLE_MS_ASSERTIONS) return;
  uint32_t key_id = kv_ptr->key_id;
  //const char* message = committing_flag_to_str(flag);
  assert(key_id < LAST_MS_NODE_PTR ||
         (key_id >= DUMMY_KEY_ID_OFFSET && key_id <=  MS_INIT_DONE_FLAG_KEY));
  if (key_id == MS_INIT_DONE_FLAG_KEY)
    my_printf(green, "Writting ms_init_done_flag, %u\n", flag);
  // MS_PTR
  if (kv_ptr->key_id < LAST_MS_NODE_PTR) {
    struct ms_ptr *kv_ms_ptr = (struct ms_ptr *) kv_ptr->value;
    struct ms_ptr *new_ms_ptr = (struct ms_ptr *) new_val;
    if (new_ms_ptr->my_key_id != key_id) {
      print_ms_ptr(new_ms_ptr);
    }

  }


}




static inline void update_ms_file(uint16_t t_id, uint32_t key_id, struct ms_sess_info *info, bool enqueue)
{
  if (CLIENT_LOGS) {
    uint64_t key_hash = CityHash128((char *) &(key_id), 4).second;
    struct key key;
    memcpy(&key, &key_hash, KEY_SIZE);

//    fprintf(client_log[t_id],
//            "%s: Stack: %u key: %u/%u, new_top key_id %u  push counter: %u pop counter: %u \n",
//            push ? "Push" : "Pull", info->stack_id, key.bkt, key_id, new_top->key_id,
//            new_top->push_counter,
//            new_top->pop_counter);
    uint16_t glob_sess = (uint16_t) (machine_id * SESSIONS_PER_MACHINE + info->real_sess_i);

//    fprintf(client_log[t_id],
//            "%u %u %s %u %u %u %u\n",
//            key.bkt, glob_sess, push ? "Push" : "Pull", info->stack_id,  new_top->push_counter,
//            new_top->pop_counter, key_id);
    if (enqueue) {
      struct ms_ptr *new_tail = &info->new_tail;
      struct ms_ptr *tail = &info->tail;

      struct ms_ptr *last_node_ptr = &info->last_or_first_node_ms_ptr;
      struct ms_ptr *new_last_node_ptr = &info->new_last_or_first_node_ms_ptr;

      fprintf(client_log[t_id],
              "|%u: E %u Tail %lu/%u --> %lu/%u "
                //"| Last %lu/%u --> %lu/%u
              "| %u/%u e/d: %u/%u\n\n",
              info->queue_id, new_tail->next_key_id,
              tail->counter, tail->next_key_id,
              new_tail->counter, new_tail->next_key_id,
              //last_node_ptr->counter, last_node_ptr->next_key_id,
              //new_last_node_ptr->counter, new_last_node_ptr->next_key_id,
              info->glob_sess_i, t_id,
              info->enqueue_num, info->dequeue_num);
    }
    else {
      struct ms_ptr *new_head = &info->new_head;
      struct ms_ptr *head = &info->head;
      fprintf(client_log[t_id],
              "|%u: D %u Head: %lu/%u --> %lu/%u | %u/%u e/d: %u/%u\n",
              info->queue_id, new_head->next_key_id,
              head->counter, head->next_key_id,
              new_head->counter, new_head->next_key_id,
              info->glob_sess_i, t_id,
              info->enqueue_num, info->dequeue_num);
    }

  }

  //fprintf(client_log[t_id], "Push: Stack: %u key: %u  push counter: %u pop counter: %u \n",
  //        info->stack_id, new_top->key_id, new_top->push_counter, new_top->pop_counter);

}

// When finding that the tail is left behind, it gets advanced. Here we log the result of that
static inline void log_the_tail_advancement(struct ms_sess_info *info, bool enqueue, uint16_t t_id)
{
  if (CLIENT_ASSERTIONS && CLIENT_LOGS) {
    if (!info->tail_left_behind) return;
    else info->tail_left_behind = false;

    poll_a_req_blocking(info->real_sess_i, info->last_req_id);
    if (info->advance_tail_result) {
      struct ms_ptr *new_tail = &info->new_tail;
      struct ms_ptr *tail = &info->tail;
      fprintf(client_log[t_id],
              "|%u: %s %u Tail %lu/%u --> %lu/%u "
                "| %u/%u e/d: %u/%u\n\n",
              info->queue_id,
              enqueue ? "E-A" : "D-A ",
              new_tail->next_key_id,
              tail->counter, tail->next_key_id,
              new_tail->counter, new_tail->next_key_id,
              info->glob_sess_i, t_id,
              info->enqueue_num, info->dequeue_num);


    }
    info->advance_tail_result = false;
  }
}

static inline void ms_init_print()
{
  printf("Number of Queues: %d, available nodes: %d, RMWable: %d, MS_WRITES_NUM: %d \n",
         MS_QUEUES_NUM, MS_NODE_NUM, NUM_OF_RMW_KEYS, MS_WRITES_NUM);
  printf("%d --> %d: tails \n", MS_TAIL_KEY_ID_OFFSET, (MS_HEAD_KEY_ID_OFFSET - 1));
  printf("%d --> %d: heads \n", MS_HEAD_KEY_ID_OFFSET, (MS_NODE_PTR_OFFSET - 1));
  printf("%d --> %d: node keys (including dummies) \n", MS_NODE_PTR_OFFSET, (LAST_MS_NODE_PTR - 1));
  printf("----------Non-RMWable---------\n");
  printf("%d --> %d: dummy nodes \n", DUMMY_KEY_ID_OFFSET, (MS_INIT_DONE_FLAG_KEY - 1));
  printf("%d: init done flag key \n", MS_INIT_DONE_FLAG_KEY);

}


// Return the key_id of the pointer of a node
static inline uint32_t ms_get_node_ptr_key_id(uint32_t node_key_id)
{
  return MS_NODE_PTR_OFFSET + ((node_key_id - DUMMY_KEY_ID_OFFSET) / MS_WRITES_NUM);
}

// compares two ms-ptrs, return true if equal
static inline bool are_ms_ptrs_equal(struct ms_ptr *ptr1, struct ms_ptr *ptr2)
{
  assert(ptr1->queue_id == ptr2->queue_id && ptr1->my_key_id == ptr2->my_key_id);
  return ptr1->next_key_id == ptr2->next_key_id &&
         ptr1->counter == ptr2->counter;
}

//
static inline bool ms_is_valid_node_key(uint32_t key_id)
{
  return key_id >= DUMMY_KEY_ID_OFFSET && key_id < MS_INIT_DONE_FLAG_KEY &&
        ((key_id - DUMMY_KEY_ID_OFFSET) % MS_WRITES_NUM == 0);
}

static inline void err_message_if_invalid_node_key(struct ms_sess_info *info, char * message,
                                                   uint32_t node_id, uint32_t key_id, uint16_t t_id)
{
  if (CLIENT_ASSERTIONS) {
    if (!ms_is_valid_node_key(key_id)) {
      my_printf(red, "Client: %u Session %u: %s: in queue: %u, owned key %u, state %u, "
                   "checking key ptr %u from node %u: %s \n",
                 t_id, info->real_sess_i,
                 info->enq_or_deq_state == MS_ENQUEUING ? "E" : "D", info->queue_id, info->owned_key,
                 info->state, key_id, node_id, message);

      if (info->enq_or_deq_state == MS_ENQUEUING) {
        struct ms_ptr *new_tail = &info->new_tail;
        struct ms_ptr *tail = &info->tail;

        struct ms_ptr *last_node_ptr = &info->last_or_first_node_ms_ptr;
        struct ms_ptr *new_last_node_ptr = &info->new_last_or_first_node_ms_ptr;

        my_printf(yellow, "|%u: E %u Last %lu/%u --> %lu/%u | %u/%u e/d: %u/%u\n\n",
                info->queue_id, tail->next_key_id,
                last_node_ptr->counter, last_node_ptr->next_key_id,
                new_last_node_ptr->counter, new_last_node_ptr->next_key_id,
                info->glob_sess_i, t_id,
                info->enqueue_num, info->dequeue_num);
      }
      else {
        struct ms_ptr *new_head = &info->new_head;
        struct ms_ptr *head = &info->head;
        my_printf(yellow, "|%u: D %u Head: %lu/%u --> %lu %u | %u/%u e/d: %u/%u\n",
                info->queue_id, new_head->next_key_id,
                head->counter, head->next_key_id,
                new_head->counter, new_head->next_key_id,
                info->glob_sess_i, t_id,
                info->enqueue_num, info->dequeue_num);
      }

      assert(false);

    }
  }

}

// Machines 1 to N, wait for machine 0 to perform the initialization
static inline void ms_wait_for_init(uint16_t t_id)
{
  uint8_t init_done_flag;
  uint16_t real_session = (uint16_t) (t_id * SESSIONS_PER_CLIENT);
  do {
    usleep(2000);
    blocking_read(MS_INIT_DONE_FLAG_KEY, &init_done_flag, 1, real_session);
  } while (init_done_flag != MS_INIT_DONE_FLAG);
  printf("CLient %u sees the INIT_DONE_FLAG raised %u \n", t_id, init_done_flag);
}

static inline void ms_set_up_tail_and_head(uint16_t t_id)
{
  struct ms_ptr tail, head, dummy_ptr;
  uint32_t dummy_key = DUMMY_KEY_ID_OFFSET, tail_key_id = MS_TAIL_KEY_ID_OFFSET,
    head_key_id = MS_HEAD_KEY_ID_OFFSET;
  //printf("Client %u uses sessions from %u to %u \n", t_id, sess_offset,  sess_offset + SESSIONS_PER_CLIENT -1);
  for(uint32_t q_i = 0; q_i < MS_QUEUES_NUM; q_i++) {
    tail.next_key_id = dummy_key;
    tail.counter = 0;
    tail.my_key_id = tail_key_id;
    head.next_key_id = dummy_key;
    head.counter = 0;
    head.my_key_id = head_key_id;
    uint16_t real_sess_i = 0;//sess_offset + s_i;
    //printf("pushing a req for key %u \n", dummy_key);
    async_write_strong(tail_key_id, (uint8_t *) &tail,
                       sizeof(struct ms_ptr), 0);

    async_write_strong(head_key_id, (uint8_t *) &head,
                       sizeof(struct ms_ptr), 0);
    uint32_t dummy_ptr_key = ms_get_node_ptr_key_id(dummy_key);
    dummy_ptr.next_key_id = 0;
    dummy_ptr.queue_id = q_i;
    dummy_ptr.my_key_id = dummy_ptr_key;
    dummy_ptr.pushed = true;
    dummy_ptr.counter = 1;

    //printf("%u %u %u \n", dummy_key, dummy_ptr_key, dummy_ptr.my_key_id);
    async_write_strong(dummy_ptr_key, (uint8_t *) &dummy_ptr,
                       sizeof(struct ms_ptr), 0);


    dummy_key += MS_WRITES_NUM; tail_key_id++; head_key_id++;
    //MOD_INCR(s_i, SESSIONS_PER_CLIENT);
  }
  printf("CLient %u initialiazed %u queues \n", t_id, MS_QUEUES_NUM);
  uint8_t init_done_flag = MS_INIT_DONE_FLAG;

  async_release_strong(MS_INIT_DONE_FLAG_KEY, &init_done_flag, 1, 0);

}

//ENQUEUE FSM
static inline void ms_enqueue_state_machine(struct ms_sess_info *info, uint16_t t_id)
{
  struct ms_node *new_node = info->new_node;
  struct ms_ptr *tail = &info->tail;
  struct ms_ptr *sec_tail = &info->second_tail;
  struct ms_ptr *new_tail = &info->new_tail;
  uint16_t real_sess_i = info->real_sess_i;
  struct ms_ptr *new_node_ptr = &info->owned_node_ptr;
  struct ms_ptr *last_node_ptr = &info->last_or_first_node_ms_ptr;
  struct ms_ptr *new_last_node_ptr = &info->new_last_or_first_node_ms_ptr;
  uint32_t tail_key_id = info->queue_id;
  new_tail->my_key_id = tail_key_id;
  if (CLIENT_ASSERTIONS) assert(real_sess_i < SESSIONS_PER_MACHINE);

  switch (info->state) {
    case MS_INIT:
     if (CLIENT_ASSERTIONS) {
       assert(!info->success);
       assert(info->enqueue_num == info->dequeue_num);
       assert(info->owned_key_ptr == ms_get_node_ptr_key_id(info->owned_key));
     }
      new_node[0].node_id = info->owned_key;
      new_node[0].node_ptr_key_id = info->owned_key_ptr;
      for (uint16_t i = 0; i < MS_WRITES_NUM; i++) {
        async_write_strong(info->owned_key + i, (uint8_t *) &new_node[i],
                           sizeof(struct ms_node), real_sess_i);
      }

      // Set the ptr of the node to be pointing to null
      if (CLIENT_ASSERTIONS) {
        new_node_ptr->pushed = true;
        new_node_ptr->my_key_id = info->owned_key_ptr;
        new_node_ptr->counter++;
        new_node_ptr->queue_id = info->queue_id;
      }
      new_node_ptr->next_key_id = 0;
      async_write_strong(info->owned_key_ptr, (uint8_t *) new_node_ptr,
                         sizeof(struct ms_ptr), info->real_sess_i);

      //break;
    case MS_LOOP_START:
      log_the_tail_advancement(info, true, t_id);
      // read tail -- this has to be acquire
      info->last_req_id = (uint32_t) async_acquire_strong(tail_key_id, (uint8_t *) tail,
                                                          sizeof(struct ms_ptr), real_sess_i);
      info->state = MS_READ_TAIL;
      break;
    case MS_READ_TAIL:
      poll_a_req_blocking(real_sess_i, info->last_req_id);
      //if (CLIENT_ASSERTIONS) assert(is_valid_node_key(tail->next_key_id));
      err_message_if_invalid_node_key(info, "reading tail when enqueuing in MS_READ_TAIL",
                                      tail_key_id, tail->next_key_id,  t_id);
      //printf("Session %u/%u read tail of %u, which points to %u/%u \n",
      //       info->glob_sess_i, t_id, info->queue_id, tail->next_key_id,
      //       get_node_ptr_key_id(tail->next_key_id));
      async_acquire_strong(ms_get_node_ptr_key_id(tail->next_key_id), (uint8_t *) last_node_ptr,
                        sizeof(struct ms_ptr), real_sess_i);
      // read tail again -- a second time
      info->last_req_id = (uint32_t) async_acquire_strong(tail_key_id, (uint8_t *) sec_tail,
                                                          sizeof(struct ms_ptr), real_sess_i);

      info->state = MS_READ_LAST_NODE;
      break;
    case MS_READ_LAST_NODE:
      poll_a_req_blocking(real_sess_i, info->last_req_id);
      if (!are_ms_ptrs_equal(tail, sec_tail)) {
        info->state = MS_LOOP_START;
        break;
      }
      //printf ("Tail and sec tail are consistent \n");
      if (CLIENT_ASSERTIONS) assert(are_ms_ptrs_equal(tail, sec_tail));
      if (last_node_ptr->next_key_id == 0) { // if it points to zero then, it is the last item in the queue
        //printf ("Tail is pointing to the last element \n");
        if (CLIENT_ASSERTIONS) {
          new_last_node_ptr->my_key_id = last_node_ptr->my_key_id;
          new_last_node_ptr->pushed = last_node_ptr->pushed;
          new_last_node_ptr->queue_id = last_node_ptr->queue_id;
          assert(last_node_ptr->queue_id == info->queue_id);
        }

        new_last_node_ptr->next_key_id = info->owned_key;
        new_last_node_ptr->counter = last_node_ptr->counter + 1;
        //if (CLIENT_ASSERTIONS) assert(is_valid_node_key(new_last_node_ptr->next_key_id));
        err_message_if_invalid_node_key(info, "reading new last_node_ptr when enqueuing in MS_READ_LAST_NODE",
                                        new_last_node_ptr->my_key_id, new_last_node_ptr->next_key_id, t_id);

        info->last_req_id = (uint32_t) async_cas_strong(ms_get_node_ptr_key_id(tail->next_key_id),
                                                        (uint8_t *) last_node_ptr, (uint8_t *) new_last_node_ptr,
                                                        sizeof(struct ms_ptr), &info->success, true, real_sess_i);
        info->state = MS_POTENTIAL_SUCCESS;
      }
      else { // if the read node does not point to zero then another node has been enqueued
        //printf ("Tail of %u is not pointing to the last element %u/%u --> %u \n", info->queue_id,
        //        tail->next_key_id, get_node_ptr_key_id(tail->next_key_id), last_node_ptr->next_key_id);
        //if (CLIENT_ASSERTIONS) assert(is_valid_node_key(last_node_ptr->next_key_id));
        err_message_if_invalid_node_key(info, "reading last_node_ptr when enqueuing in MS_READ_LAST_NODE",
                                        last_node_ptr->my_key_id, last_node_ptr->next_key_id, t_id);
        new_tail->next_key_id = last_node_ptr->next_key_id;
        new_tail->counter = tail->counter + 1;
        info->advance_tail_result = false;
        info->tail_left_behind = true;
        info->last_req_id = (uint32_t) async_cas_strong(tail_key_id, (uint8_t *) tail, (uint8_t *) new_tail,
                                                        sizeof(struct ms_ptr), &info->advance_tail_result,
                                                        true, real_sess_i);
        info->state = MS_LOOP_START;
      }
      break;
    case MS_POTENTIAL_SUCCESS:
      poll_a_req_blocking(real_sess_i, info->last_req_id);
      if (info->success) {
        new_tail->next_key_id = info->owned_key;
        //if (CLIENT_ASSERTIONS) assert(is_valid_node_key(new_tail->next_key_id));
        err_message_if_invalid_node_key(info, "reading new_tail when enqueuing in MS_POTENTIAL_SUCCESS",
                                        tail_key_id, new_tail->next_key_id, t_id);
        new_tail->counter = tail->counter + 1;
        (uint32_t) async_cas_strong(tail_key_id, (uint8_t *) tail, (uint8_t *) new_tail,
                                    sizeof(struct ms_ptr), &info->advance_tail_result, true, real_sess_i);

        //my_printf(green, "Session %u/%u enqueues key %u to stack %u \n",
        //             info->glob_sess_i, t_id, info->owned_key, info->queue_id);
        update_ms_file(t_id, 0, info, true);
        info->success = false;
        info->enqueue_num++;
        c_stats[t_id].microbench_pushes++;
        if (CLIENT_ASSERTIONS) assert(c_stats[t_id].microbench_pushes > c_stats[t_id].microbench_pops);
        info->enq_or_deq_state = MS_DEQUEUING;
        info->state = MS_INIT;
      }
      else {
        info->state = MS_LOOP_START;
        break;
      }
      break;
    default: if (CLIENT_ASSERTIONS) assert(false);
  }
}

static inline void ms_dequeue_state_machine(struct ms_sess_info *info,
                                            uint32_t *queue_id_cntr,
                                            uint16_t t_id)
{
  struct ms_ptr *tail = &info->tail;
  struct ms_ptr *new_tail = &info->new_tail;
  struct ms_ptr *head = &info->head;
  struct ms_ptr *sec_head = &info->second_head;
  struct ms_ptr *new_head = &info->new_head;
  struct ms_node *new_node = info->new_node;
  struct ms_ptr *first_node_ptr = &info->owned_node_ptr;

  uint16_t real_sess_i = info->real_sess_i;
  uint32_t head_key_id = info->queue_id + MS_HEAD_KEY_ID_OFFSET;
  uint32_t tail_key_id = info->queue_id;
  new_tail->my_key_id = tail_key_id;
  new_head->my_key_id = head_key_id;

  if (CLIENT_ASSERTIONS) assert(real_sess_i < SESSIONS_PER_MACHINE);

  switch (info->state) {
    case MS_INIT:
      if (CLIENT_ASSERTIONS) assert(info->enqueue_num == info->dequeue_num + 1);
    case MS_LOOP_START:
      log_the_tail_advancement(info, false, t_id);
      // read head
      info->last_req_id = (uint32_t) async_acquire_strong(head_key_id, (uint8_t *) head,
                                                          sizeof(struct ms_ptr), real_sess_i);
      // read tail
      async_read_strong(tail_key_id, (uint8_t *) tail, sizeof(struct ms_ptr), real_sess_i);
      info->state = MS_READ_HEAD;
      break;
    case MS_READ_HEAD:
      poll_a_req_blocking(real_sess_i, info->last_req_id);
      err_message_if_invalid_node_key(info, "reading head when dequeuing in MS_READ_HEAD",
                                      head_key_id, head->next_key_id, t_id);

      // read first node
      async_acquire_strong(ms_get_node_ptr_key_id(head->next_key_id), (uint8_t *) first_node_ptr,
                           sizeof(struct ms_ptr), real_sess_i);
      // read head again -- a second time
      info->last_req_id = (uint32_t) async_acquire_strong(head_key_id, (uint8_t *) sec_head,
                                                          sizeof(struct ms_ptr), real_sess_i);
      info->state = MS_READ_FIRST_NODE;
      break;
    case MS_READ_FIRST_NODE:
      poll_a_req_blocking(real_sess_i, info->last_req_id);
      err_message_if_invalid_node_key(info, "reading sec_head when dequeuing in MS_READ_FIRST_NODE",
                                      head_key_id, sec_head->next_key_id, t_id);
      if (!are_ms_ptrs_equal(head, sec_head)) {
        info->state = MS_LOOP_START;
        break;
      }

      if (first_node_ptr->next_key_id == 0 || first_node_ptr->queue_id != info->queue_id)
        print_ms_ptr(first_node_ptr);

      if (CLIENT_ASSERTIONS) assert(are_ms_ptrs_equal(head, sec_head));
      if (head->next_key_id == tail->next_key_id) {
        if (CLIENT_ASSERTIONS && first_node_ptr->next_key_id == 0) { // If the queue is empty
          my_printf(red, "Session %u/%u tries to dequeue from %u/%u, first node_ptr %u/%u points to 0 \n",
                     info->glob_sess_i, t_id, info->queue_id, head->counter, head->next_key_id,
                     ms_get_node_ptr_key_id(head->next_key_id));
          exit(0);
          assert(false); // DUMMY has to point somewhere, because an enqueue has already happened
        }
        else { //if tail is slacking advance it
          new_tail->next_key_id = first_node_ptr->next_key_id;
          new_tail->counter = tail->counter + 1;
          info->advance_tail_result = false;
          info->tail_left_behind = true;
          info->last_req_id = (uint32_t) async_cas_strong(tail_key_id, (uint8_t *) tail, (uint8_t *) new_tail,
                                                         sizeof(struct ms_ptr), &info->advance_tail_result,
                                                         true, real_sess_i);
        }
        info->state = MS_LOOP_START;
      }
      else { // if does not point to the same place as tail
        // do the data reads
        for (uint16_t i = 0; i < MS_WRITES_NUM; i++) {
          async_read_strong(head->next_key_id + i, (uint8_t *) &new_node[i],
                             sizeof(struct ms_node), real_sess_i);
        }
        // try the CAS
        new_head->next_key_id = first_node_ptr->next_key_id;
        new_head->counter = head->counter + 1;
        err_message_if_invalid_node_key(info, "head points to a different node than tail,"
                                          " but the node points to null, MS_READ_FIRST_NODE",
                                        head->next_key_id, first_node_ptr->next_key_id, t_id);
        info->last_req_id = (uint32_t) async_cas_strong(head_key_id, (uint8_t *) head, (uint8_t *) new_head,
                                                       sizeof(struct ms_ptr), &info->success, true, real_sess_i);
        info->state = MS_POTENTIAL_SUCCESS;
      }
      break;
    case MS_POTENTIAL_SUCCESS:
      poll_a_req_blocking(real_sess_i, info->last_req_id);
      if (info->success) {
        info->owned_key = head->next_key_id;
        info->owned_key_ptr = ms_get_node_ptr_key_id(info->owned_key);
        if (CLIENT_ASSERTIONS) {
          assert(first_node_ptr->pushed);
          assert(first_node_ptr->my_key_id == info->owned_key_ptr);
          assert(first_node_ptr->queue_id == info->queue_id);
          err_message_if_invalid_node_key(info, "reading new_head when dequeuing in MS_POTENTIAL_SUCCESS",
                                          head_key_id, new_head->next_key_id, t_id);
          first_node_ptr->pushed = false;
          first_node_ptr->counter++;
          async_write_strong(first_node_ptr->my_key_id, (uint8_t *) first_node_ptr,
                             sizeof(struct ms_ptr), info->real_sess_i);
        }
        update_ms_file(t_id, 0, info, false);
        info->success = false;
        info->dequeue_num++;
        c_stats[t_id].microbench_pops++;
        if (CLIENT_ASSERTIONS) assert(c_stats[t_id].microbench_pushes >= c_stats[t_id].microbench_pops);
        if (!MS_NO_CONFLICT) {
          info->queue_id = *queue_id_cntr;
          MOD_INCR(*queue_id_cntr, MS_QUEUES_NUM);
        }
        info->enq_or_deq_state = MS_ENQUEUING;
        info->state = MS_INIT;
      }
      else {
        info->state = MS_LOOP_START;
        break;
      }
      break;
    default: if (CLIENT_ASSERTIONS) assert(false);
  }
}

//
static inline void ms_enqueue_dequeue_multi_session(uint16_t t_id)
{

  if (machine_id == 0 && t_id == 0)
    ms_set_up_tail_and_head(t_id);
  else ms_wait_for_init(t_id);

  assert(sizeof(struct ms_ptr) <= VALUE_SIZE);
  assert(sizeof(struct ms_node) <= VALUE_SIZE);
  assert(MS_WRITES_NUM > 0);
  uint16_t s_i = 0;
  uint16_t sess_offset = (uint16_t) (t_id * SESSIONS_PER_CLIENT);
  assert(sess_offset + SESSIONS_PER_CLIENT <= SESSIONS_PER_MACHINE);
  struct ms_sess_info *info = calloc(SESSIONS_PER_CLIENT, sizeof(struct ms_sess_info));



  uint32_t key_offset = (uint32_t) (MS_NODE_KEY_OFFSET + (MS_WRITES_NUM * (SESSIONS_PER_MACHINE * machine_id)));
  uint32_t first_key = key_offset + sess_offset * MS_WRITES_NUM;

  uint32_t last_key = key_offset + ((sess_offset + (SESSIONS_PER_CLIENT -1)) * MS_WRITES_NUM);
  uint16_t min_sess = sess_offset, max_sess = (uint16_t) (sess_offset + (SESSIONS_PER_CLIENT -1));
  uint16_t min_wrkr = (uint16_t) (min_sess / SESSIONS_PER_THREAD),
    max_wrkr = (uint16_t) (max_sess / SESSIONS_PER_THREAD);

  uint32_t queue_id_cntr = ((machine_id * 100) + last_key) % MS_QUEUES_NUM;
  printf("Client %u: sessions %u to %u, workers %u to %u, keys %u to %u, globally max key %u \n",
         t_id, min_sess, max_sess, min_wrkr, max_wrkr,
         first_key, last_key, (MS_INIT_DONE_FLAG_KEY - 1));
  if (machine_id == 0 && t_id == 0) ms_init_print();


  for (s_i = 0; s_i < SESSIONS_PER_CLIENT; s_i++) {
    info[s_i].state = MS_INIT;
    info[s_i].enq_or_deq_state = MS_ENQUEUING;
    info[s_i].s_i = s_i;
    info[s_i].real_sess_i = sess_offset + s_i;
    info[s_i].glob_sess_i = (uint16_t) (SESSIONS_PER_MACHINE * machine_id + info[s_i].real_sess_i);
    info[s_i].owned_key = (uint32_t) (MS_NODE_KEY_OFFSET + (info[s_i].glob_sess_i * MS_WRITES_NUM));
    info[s_i].owned_key_ptr = ms_get_node_ptr_key_id(info[s_i].owned_key);
    //printf("Session %u key %u \n", info[s_i].real_sess_i, info[s_i].owned_key_ptr);
    if (CLIENT_ASSERTIONS) assert(info[s_i].real_sess_i < SESSIONS_PER_MACHINE);
    info[s_i].new_node = calloc(MS_WRITES_NUM, (sizeof(struct ms_node)));
    info[s_i].last_or_first_node = calloc(MS_WRITES_NUM, (sizeof(struct ms_node)));
    info[s_i].queue_id = info[s_i].glob_sess_i;
    info[s_i].advance_tail_result = false;
    info[s_i].tail_left_behind = false;
    assert(info[s_i].queue_id < MS_QUEUES_NUM);
  }

  s_i = 0;
  while(true) {
    switch (info[s_i].enq_or_deq_state) {
      case MS_ENQUEUING:
        ms_enqueue_state_machine(&info[s_i],  t_id);
        break;
      case MS_DEQUEUING:
        ms_dequeue_state_machine(&info[s_i], &queue_id_cntr, t_id);
        break;
      default: if (CLIENT_ASSERTIONS) assert(false);
    }
    MOD_INCR(s_i, SESSIONS_PER_CLIENT);
  }
}


/* ------------------------------------------------------------------------------------------------------------------- */
/*------------------------------H&M LIST------------------------------------------------------------------------------*/
/* ------------------------------------------------------------------------------------------------------------------- */
#define HM_INSERTING 0
#define HM_DELETING 1
#define HM_CLEAN_UP 2
#define HM_SEARCHING_INS 3
#define HM_SEARCHING_DEL 4
#define HM_SEARCHING_CL 5


//HM_STATES
#define HM_INIT 0
#define HM_SEARCH_INNER_LOOP 1
#define HM_TRY_INSERT 2
#define HM_TRY_DELETE 12
#define HM_READ_CURR_PTR_AGAIN 3
#define HM_POTENTIAL_SUCCESS 4
#define HM_SEARCH_INNER_LOOP_AFTER_COPYING_PTRS 5 // this state is for when traversing the list, where the curr_ptr is copied from next ptr
#define HM_MARKED_POTENTIAL_SUCCESS 6 // after the delete attempts to mark a node
#define HM_CL_AFTER_SEARCH 2
#define HM_READ_FIRST_NODE 3


#define HM_MAX_WAIT_LOOPS 100
#define HM_NODE_SIZE (VALUE_SIZE - 8)
#define HM_PTR_SIZE 12;


// KEY ALLOCATION-- SIZES & OFFSETS
#define HM_LISTS_NUM (GLOBAL_SESSION_NUM)
#define HM_PREINSERTED_NODE_NUM (HM_LISTS_NUM) // 1 inserted in each list to begin with and
#define HM_FREE_NODE_NUM (GLOBAL_SESSION_NUM) // 1 per session beyond that
#define HM_NODE_NUM (HM_PREINSERTED_NODE_NUM + HM_FREE_NODE_NUM)


#define HM_HEAD_KEY_ID_OFFSET 0 // there is one head per list, totally: HM_LISTS_NUM heads
#define HM_PREINSERTED_NODE_PTR_OFFSET (HM_HEAD_KEY_ID_OFFSET + HM_LISTS_NUM)
#define HM_FREE_NODE_PTR_OFFSET (HM_PREINSERTED_NODE_PTR_OFFSET + PREINSERTED_NODE_NUM)
#define LAST_HM_NODE_PTR (HM_PREINSERTED_NODE_PTR_OFFSET + HM_NODE_NUM)


#define HM_PREINSERTED_NODE_KEY_OFFSET (NUM_OF_RMW_KEYS)
#define HM_FREE_NODE_KEY_OFFSET (HM_PREINSERTED_NODE_KEY_OFFSET + (HM_WRITES_NUM * HM_PREINSERTED_NODE_NUM))
#define HM_INIT_DONE_FLAG_KEY (HM_FREE_NODE_KEY_OFFSET + (HM_WRITES_NUM * HM_FREE_NODE_NUM))

#define HM_INIT_DONE_FLAG 162


/* ----Key allocation----
 * 0 -> HM_LISTS_NUM                 : heads
 * HM_NODE_PTR_OFFSET -> HM_NODE_PTR_OFFSET + HM_PREINSERTED_NODE_NUM   : pointers of preinserted nodes
 * HM_FREE_NODE_PTR_OFFSET -> HM_FREE_NODE_PTR_OFFSET + HM_FREE_NODE_NUM   : pointers of free nodes
 * ---NON-RMWABLE
 * HM_PREINSERTED_NODE_KEY_OFFSET -> HM_PREINSERTED_NODE_KEY_OFFSET + (HM_PREINSERTED_NODE_NUM * HM_WRITES_NUM) : bodies of preinserted nodes
 * HM_FREE_NODE_KEY_OFFSET : HM_FREE_NODE_KEY_OFFSET + (HM_WRITES_NUM * HM_FREE_NODE_NUM) : bodies of free nodes
 * HM_FREE_NODE_KEY_OFFSET + (HM_WRITES_NUM * HM_FREE_NODE_NUM) : init_done flag (INIT_DONE_FLAG_KEY)
 *
 * */

struct hm_ptr {
  bool pushed;
  bool marked;
  uint32_t list_id;
  uint32_t my_key_id;
  uint32_t next_key_id;
  uint64_t counter;
};

struct hm_node {
  uint8_t value[HM_NODE_SIZE];
  uint32_t node_id;
  uint32_t node_ptr_key_id; // used for comparison when searching/inserting/deleting
};

struct hm_sess_info {
  uint8_t state;
  uint8_t ins_or_del_state;
  bool success;
  bool search_found;
  uint16_t real_sess_i;
  uint16_t glob_sess_i;
  uint32_t last_req_id;
  uint32_t list_id;
  uint32_t owned_key; // they key of the owned hm_node
  uint32_t owned_key_ptr;
  uint32_t insert_num;
  uint32_t delete_num;
  uint32_t wait_cntr;
  struct hm_node *new_node;

  struct hm_ptr new_node_ptr;
  uint32_t prev_ptr;
  struct hm_ptr curr_ptr;
  struct hm_ptr new_curr_ptr;
  struct hm_ptr hlp_curr_ptr;
  struct hm_ptr next_ptr;
  struct hm_node next_node;
};

static inline bool hm_ptrs_are_equal(struct hm_ptr *ptr1, struct hm_ptr *ptr2)
{
  if (CLIENT_ASSERTIONS) {
    assert(ptr1->my_key_id == ptr2->my_key_id);
  }

  return ptr1->next_key_id == ptr2->next_key_id &&
         ptr1->counter == ptr2->counter &&
         ptr1->marked == ptr2->marked;
}

static inline void print_hm_ptr(struct hm_ptr *hm_ptr, const char *ptr_name)
{
  printf("%s: pushed %u, list id %u, my_key_id %u, counter %lu, marked %u, next_key_id %u\n",
         ptr_name, hm_ptr->pushed, hm_ptr->list_id, hm_ptr->my_key_id, hm_ptr->counter, hm_ptr->marked, hm_ptr->next_key_id);
}

// Return the key_id of the pointer of a node
static inline uint32_t hm_get_node_ptr_key_id(uint32_t node_key_id)
{
  if (CLIENT_ASSERTIONS) assert(node_key_id >= HM_PREINSERTED_NODE_KEY_OFFSET);
  return HM_PREINSERTED_NODE_PTR_OFFSET + ((node_key_id - HM_PREINSERTED_NODE_KEY_OFFSET) / HM_WRITES_NUM);
}

// Return the key_id of  a node from its ptr id
static inline uint32_t hm_get_node_key_id_from_ptr(uint32_t node_ptr_key_id)
{
  if (CLIENT_ASSERTIONS) assert(node_ptr_key_id >= HM_PREINSERTED_NODE_PTR_OFFSET);
  return HM_PREINSERTED_NODE_KEY_OFFSET + ((node_ptr_key_id - HM_PREINSERTED_NODE_PTR_OFFSET));
}

static inline bool hm_is_valid_node_key(uint32_t key_id)
{
  return key_id >= HM_PREINSERTED_NODE_KEY_OFFSET && key_id < HM_INIT_DONE_FLAG_KEY &&
         ((key_id - HM_PREINSERTED_NODE_KEY_OFFSET) % HM_WRITES_NUM == 0);
}

static inline bool check_hm_is_valid_node_ptr_key(uint32_t key_id)
{
  if (CLIENT_ASSERTIONS)
    assert(key_id >= HM_PREINSERTED_NODE_PTR_OFFSET && key_id < LAST_HM_NODE_PTR);
}

// Reset
static inline void reset_info_state_after_searching(struct hm_sess_info *info, uint16_t t_id)
{
  struct hm_ptr *curr_ptr = &info->curr_ptr;
  info->wait_cntr = 0;
  if (info->ins_or_del_state == HM_SEARCHING_INS) {
    info->ins_or_del_state = HM_INSERTING;
    info->state = HM_TRY_INSERT;
//    my_printf(green, "Search-in: Cl: %u/%u, next_key_id %u, "
//                   "curr_key_id %u, list %u/%u, pushed/marked %d/%d \n",
//                  t_id, info->glob_sess_i, curr_ptr->next_key_id, curr_ptr->my_key_id,
//                  info->list_id, curr_ptr->list_id, curr_ptr->pushed, curr_ptr->marked);
  }
  else if (info->ins_or_del_state == HM_SEARCHING_DEL){
    //if (CLIENT_ASSERTIONS) assert(info->ins_or_del_state == HM_SEARCHING_DEL);
    info->ins_or_del_state = HM_DELETING;
    info->state = HM_TRY_DELETE;
    if (CLIENT_ASSERTIONS) assert(info->curr_ptr.next_key_id != 0);
  }
  else {
    if (CLIENT_ASSERTIONS) assert(info->ins_or_del_state == HM_SEARCHING_CL);
    info->ins_or_del_state = HM_DELETING;
    info->state = HM_CL_AFTER_SEARCH;
  }
}

static inline void hm_init_print()
{
  printf("Number of Lists: %d, available nodes: %d, RMWable: %d, HM_WRITES_NUM: %d \n",
         HM_LISTS_NUM, HM_NODE_NUM, NUM_OF_RMW_KEYS, HM_WRITES_NUM);
  printf("%d --> %d: heads \n", HM_HEAD_KEY_ID_OFFSET, (HM_PREINSERTED_NODE_PTR_OFFSET - 1));
  printf("%d --> %d: node keys  \n", HM_PREINSERTED_NODE_PTR_OFFSET, (LAST_HM_NODE_PTR - 1));
  printf("----------Non-RMWable---------\n");
  printf("%d --> %d: nodes \n", HM_PREINSERTED_NODE_KEY_OFFSET, (HM_INIT_DONE_FLAG_KEY - 1));
  printf("%d: init done flag key \n", HM_INIT_DONE_FLAG_KEY);

}

// One client thread is responsible to initialize all the lists, while the rest wait for it (by polling on a flag)
static inline void hm_set_up_lists(uint16_t t_id)
{
  assert(NUM_OF_RMW_KEYS > (HM_LISTS_NUM + HM_NODE_NUM));
  struct hm_ptr head, node_ptr;
  struct hm_node node;
  memset(node.value, 0, (uint32_t) HM_NODE_SIZE);
  uint32_t node_key = HM_PREINSERTED_NODE_KEY_OFFSET,
    head_key_id = HM_HEAD_KEY_ID_OFFSET;
  //printf("Client %u uses sessions from %u to %u \n", t_id, sess_offset,  sess_offset + SESSIONS_PER_CLIENT -1);
  for(uint32_t l_i = 0; l_i < HM_LISTS_NUM; l_i++) {

    // Write heads
    head.pushed = true;
    head.marked = false;
    head.list_id = l_i;
    head.my_key_id = l_i;
    head.next_key_id = node_key;
    head.counter = 0;
    async_write_strong(head_key_id, (uint8_t *) &head,
                       sizeof(struct hm_ptr), 0);

    // Write node ptrs ofr pre-inserted nodes
    uint32_t node_ptr_key = hm_get_node_ptr_key_id(node_key);
    node_ptr.next_key_id = 0;
    node_ptr.list_id = l_i;
    node_ptr.my_key_id = node_ptr_key;
    node_ptr.marked = false;
    node_ptr.pushed = true;
    node_ptr.counter = 0;
    //printf("%u %u %u \n", dummy_key, dummy_ptr_key, dummy_ptr.my_key_id);
    async_write_strong(node_ptr_key, (uint8_t *) &node_ptr,
                       sizeof(struct hm_ptr), 0);

    // write preinserted nodes
    node.node_ptr_key_id = node_ptr_key;
    node.node_id = node_key;
    async_write_strong(node_key, (uint8_t *) &node,
                       sizeof(struct hm_node), 0);


    node_key += HM_WRITES_NUM; head_key_id++;
  }

  printf("CLient %u initialiazed %u lists \n", t_id, HM_LISTS_NUM);
  uint8_t init_done_flag = HM_INIT_DONE_FLAG;
  async_release_strong(HM_INIT_DONE_FLAG_KEY, &init_done_flag, 1, 0);
}


// Machines 1 to N, wait for machine 0 to perform the initialization
static inline void hm_wait_for_init(uint16_t t_id)
{
  uint8_t init_done_flag;
  uint16_t real_session = (uint16_t) (t_id * SESSIONS_PER_CLIENT);
  do {
    usleep(2000);
    blocking_read(HM_INIT_DONE_FLAG_KEY, &init_done_flag, 1, real_session);
  } while (init_done_flag != HM_INIT_DONE_FLAG);
  printf("Client %u sees the INIT_DONE_FLAG raised %u \n", t_id, init_done_flag);
}

// When inserting
static inline void hm_fill_new_node_ptr_new_curr_ptr(struct hm_sess_info *info)
{

  struct hm_ptr *new_node_ptr = &info->new_node_ptr;
  struct hm_ptr *curr_ptr = &info->curr_ptr;
  struct hm_ptr *new_curr_ptr = &info->new_curr_ptr;


  // test curr and next
  if (CLIENT_ASSERTIONS) {
    assert(!curr_ptr->marked);
    assert(curr_ptr->pushed);
    assert(info->prev_ptr == curr_ptr->my_key_id);
  }

  // fill new_node_ptr
  new_node_ptr->pushed = true;
  new_node_ptr->list_id = info->list_id;
  new_node_ptr->my_key_id = info->owned_key_ptr;

  new_node_ptr->counter = 0;
  new_node_ptr->marked = false;
  new_node_ptr->next_key_id = curr_ptr->next_key_id; // TODO check this

  new_curr_ptr->pushed = true;
  new_curr_ptr->list_id = info->list_id;
  new_curr_ptr->my_key_id = curr_ptr->my_key_id;

  new_curr_ptr->counter = curr_ptr->counter + 1;
  new_curr_ptr->marked = false;
  new_curr_ptr->next_key_id = info->owned_key; // TODO check this
}

static inline void hm_transition_to_searching(struct hm_sess_info *info)
{
  struct hm_ptr *curr_ptr = &info->curr_ptr;
  uint32_t head_key_id = info->list_id;
  uint16_t real_sess_i = info->real_sess_i;
  info->prev_ptr = head_key_id;
  info->search_found = false;
  info->last_req_id = (uint32_t) async_acquire_strong(info->prev_ptr, (uint8_t *) curr_ptr,
                                                      sizeof(struct hm_ptr), real_sess_i);
  info->next_ptr.my_key_id = 0;
  info->next_ptr.next_key_id = 0;

  info->state = HM_SEARCH_INNER_LOOP;
  if (info->ins_or_del_state == HM_INSERTING) info->ins_or_del_state = HM_SEARCHING_INS;
  else if (info->ins_or_del_state == HM_DELETING) info->ins_or_del_state = HM_SEARCHING_DEL;
  else if (info->ins_or_del_state == HM_CLEAN_UP) info->ins_or_del_state = HM_SEARCHING_CL;
}


// Searching for new_node_ptr->my_key_id
static inline void hm_search_state_machine(struct hm_sess_info *info, uint16_t t_id)
{
  struct hm_ptr *new_node_ptr = &info->new_node_ptr;
  struct hm_ptr *curr_ptr = &info->curr_ptr;
  struct hm_ptr *new_curr_ptr = &info->new_curr_ptr;
  struct hm_ptr *next_ptr = &info->next_ptr;
  struct hm_node *next_node = &info->next_node;

  uint16_t real_sess_i = info->real_sess_i;
  if (CLIENT_ASSERTIONS) assert(real_sess_i < SESSIONS_PER_MACHINE);

  switch (info->state) {
    case HM_INIT:
      poll_a_req_blocking(real_sess_i, info->last_req_id);
      hm_transition_to_searching(info);
      break;
    case HM_SEARCH_INNER_LOOP:
      poll_a_req_blocking(real_sess_i, info->last_req_id);
    case HM_SEARCH_INNER_LOOP_AFTER_COPYING_PTRS:
      // if we reached the end of the list
      if (curr_ptr->next_key_id == 0) {
        info->search_found = false;
        reset_info_state_after_searching(info, t_id);
        break;
      }

      // read the next pointer
      uint32_t next_ptr_key_id = hm_get_node_ptr_key_id(curr_ptr->next_key_id);
      async_acquire_strong(next_ptr_key_id, (uint8_t *) next_ptr,
                           sizeof(struct hm_ptr), real_sess_i);

      // read the next value, to be able to compare with searched value
      if (CLIENT_ASSERTIONS) assert(hm_is_valid_node_key(curr_ptr->next_key_id));
      async_read_strong(curr_ptr->next_key_id, (uint8_t *) next_node,
                           sizeof(struct hm_node), real_sess_i);

      // Read the Current ptr again
      info->last_req_id =
        (uint32_t) async_acquire_strong(info->prev_ptr, (uint8_t *) new_curr_ptr,
                                        sizeof(struct hm_ptr), real_sess_i);

      info->state = HM_READ_CURR_PTR_AGAIN;
      break;

    case HM_READ_CURR_PTR_AGAIN:
      poll_a_req_blocking(real_sess_i, info->last_req_id);
      if (CLIENT_ASSERTIONS) {
        assert(!curr_ptr->marked);
        assert(curr_ptr->list_id == info->list_id);
      }
      if (!hm_ptrs_are_equal(curr_ptr, new_curr_ptr)) {
        // start over
        hm_transition_to_searching(info);
        break;
      }
      else { // curr and new curr are equal
        if (CLIENT_ASSERTIONS) {
          if (next_ptr->list_id != info->list_id) {
            my_printf(red, "Cl : %u/%u, prev_ptr/curr_ptr/curr_ptr->next/next_ptr %u/%u/%u/%u list: %u/%u/%u \n",
                       t_id, info->glob_sess_i, info->prev_ptr, curr_ptr->my_key_id, curr_ptr->next_key_id,
                       next_ptr->my_key_id, info->list_id, curr_ptr->list_id, next_ptr->list_id);
          }
          assert(next_ptr->list_id == info->list_id);
        }

        if (next_ptr->marked) {
          // Attempt to help
          if (next_ptr->my_key_id == info->owned_key_ptr ||
              info->wait_cntr >= HM_MAX_WAIT_LOOPS) {
            new_curr_ptr->counter++;
            new_curr_ptr->next_key_id = next_ptr->next_key_id;
            if (CLIENT_ASSERTIONS) {
              assert(info->prev_ptr == curr_ptr->my_key_id);
              assert(next_ptr->next_key_id != curr_ptr->next_key_id);
            }

            info->last_req_id =
              (uint32_t) async_cas_strong(info->prev_ptr, (uint8_t *) curr_ptr, (uint8_t *) new_curr_ptr,
                                          sizeof(struct hm_ptr), &info->success, true, real_sess_i);
            info->wait_cntr = 0;
          }
          else info->wait_cntr++;
          hm_transition_to_searching(info);
          break;
        }
        else { // COMPARING
          check_hm_is_valid_node_ptr_key(next_node->node_ptr_key_id);
          if (CLIENT_ASSERTIONS)
            assert(next_node->node_ptr_key_id == hm_get_node_ptr_key_id(curr_ptr->next_key_id));
          if (next_node->node_ptr_key_id >= new_node_ptr->my_key_id) { //found a node with bigger id
            if (CLIENT_ASSERTIONS) {
              if (next_node->node_ptr_key_id > new_node_ptr->my_key_id) {
                info->search_found = false;
                assert (info->ins_or_del_state != HM_SEARCHING_DEL); // if unequal, it better be an insert
              }
              else {
                info->search_found = true;
                assert (info->ins_or_del_state != HM_SEARCHING_INS); // if equal it better be a delete
              }
            }
            reset_info_state_after_searching(info, t_id);
            break;
          }
        }
      }
      // This point is reached if the search must advance, in which case we advance the curr ptr to next ptr
      info->prev_ptr = next_ptr->my_key_id;
      check_hm_is_valid_node_ptr_key(info->prev_ptr);
      if (CLIENT_ASSERTIONS) {
        assert(curr_ptr->list_id == info->list_id);
        assert(next_ptr->list_id == info->list_id);
      }
      memcpy(curr_ptr, next_ptr, sizeof(struct hm_ptr));
      info->state = HM_SEARCH_INNER_LOOP_AFTER_COPYING_PTRS;
      break;
    default: if (CLIENT_ASSERTIONS) assert(false);
  }
}

//Insert FSM
static inline void hm_insert_state_machine(struct hm_sess_info *info, uint16_t t_id)
{
  struct hm_node *new_node = info->new_node;
  struct hm_ptr *new_node_ptr = &info->new_node_ptr;
  struct hm_ptr *curr_ptr = &info->curr_ptr;
  struct hm_ptr *new_curr_ptr = &info->new_curr_ptr;

  uint16_t real_sess_i = info->real_sess_i;
  if (CLIENT_ASSERTIONS) assert(real_sess_i < SESSIONS_PER_MACHINE);
//
  switch (info->state) {
    case HM_INIT:
      if (CLIENT_ASSERTIONS) {
        assert(info->insert_num == info->delete_num);
        assert(info->owned_key_ptr == hm_get_node_ptr_key_id(info->owned_key));
      }
      new_node[0].node_id = info->owned_key;
      new_node[0].node_ptr_key_id = info->owned_key_ptr;
      for (uint16_t i = 0; i < HM_WRITES_NUM; i++) {
        async_write_strong(info->owned_key + i, (uint8_t *) &new_node[i],
                           sizeof(struct hm_node), real_sess_i);
      }
      //go into searching
      hm_transition_to_searching(info);
      new_node_ptr->my_key_id = info->owned_key_ptr;
      break;
    case HM_TRY_INSERT:
      hm_fill_new_node_ptr_new_curr_ptr(info);

      async_write_strong(info->owned_key_ptr,(uint8_t *) new_node_ptr,
                         sizeof(struct hm_ptr), real_sess_i);
      // CAS the immediately previous node to point to the newly inserted one
      info->last_req_id = (uint32_t) async_cas_strong(info->prev_ptr, (uint8_t *) curr_ptr, (uint8_t *) new_curr_ptr,
                                                      sizeof(struct hm_ptr), &info->success,
                                                      true, real_sess_i);

      info->state = HM_POTENTIAL_SUCCESS;
      break;
    case HM_POTENTIAL_SUCCESS:
      poll_a_req_blocking(real_sess_i, info->last_req_id);
      if (info->success) {
        info->insert_num++;
        info->state = HM_INIT;
        info->ins_or_del_state = HM_DELETING;
        c_stats[t_id].microbench_pushes++;
        //my_printf(green, "Cl %u/%u I %u/%u\n", t_id,
        //              info->glob_sess_i, info->owned_key_ptr, info->list_id);
//        printf("Client %u, sess %u inserted %lu \n",
//               t_id, info->glob_sess_i, c_stats[t_id].microbench_pushes);
        if (CLIENT_ASSERTIONS)
          assert(c_stats[t_id].microbench_pushes > c_stats[t_id].microbench_pops);
      }
      else {// if the CAS was not successful read the head and transition to searching
        hm_transition_to_searching(info);
      }
      break;
    default: if (CLIENT_ASSERTIONS) assert(false);
  }
}


// Delete FSM
static inline void hm_delete_state_machine(struct hm_sess_info *info,
                                           uint32_t *queue_id_cntr,
                                            uint16_t t_id)
{
  struct hm_node *new_node = info->new_node;
  struct hm_ptr *new_node_ptr = &info->new_node_ptr;
  struct hm_ptr *curr_ptr = &info->curr_ptr;
  struct hm_ptr *new_curr_ptr = &info->new_curr_ptr;
  struct hm_ptr *next_ptr = &info->next_ptr;

  uint16_t real_sess_i = info->real_sess_i;

  uint32_t delete_node_ptr_id;
  if (CLIENT_ASSERTIONS) assert(real_sess_i < SESSIONS_PER_MACHINE);

  switch (info->state) {
    case HM_INIT:
      if (CLIENT_ASSERTIONS) {
        assert(info->insert_num == info->delete_num + 1);
        assert(info->owned_key_ptr == hm_get_node_ptr_key_id(info->owned_key));
      }
      //go into searching
      hm_transition_to_searching(info);
      new_node_ptr->my_key_id = info->owned_key_ptr;
      break;
    case HM_TRY_DELETE:
      // attempt to mark the node as deleted
      if (CLIENT_ASSERTIONS) {
        assert(info->state == HM_TRY_DELETE && info->ins_or_del_state == HM_DELETING);
        if (curr_ptr->next_key_id == 0) {
          printf("Client %u/%u, curr_ptr->next_key_id %u, curr->my_key_id %u, list %u/%u \n",
                 t_id, info->glob_sess_i, curr_ptr->next_key_id, curr_ptr->my_key_id,
                 info->list_id, curr_ptr->list_id);
          assert(false);
        }
      }
      delete_node_ptr_id = hm_get_node_ptr_key_id(curr_ptr->next_key_id);
      if (CLIENT_ASSERTIONS) {
        assert(delete_node_ptr_id == info->owned_key_ptr);
        assert(delete_node_ptr_id == next_ptr->my_key_id);
        assert(next_ptr->my_key_id != 0);
      }
      new_node_ptr->my_key_id = delete_node_ptr_id;
      new_node_ptr->pushed = true;
      new_node_ptr->next_key_id = next_ptr->next_key_id;
      new_node_ptr->marked = true;
      new_node_ptr->list_id = info->list_id;

      for (uint8_t i = 0; i < HM_WRITES_NUM; i++)
        async_read_strong(curr_ptr->next_key_id + i, (uint8_t*) new_node,
                          sizeof(struct hm_node), real_sess_i);

      info->last_req_id =
        (uint32_t) async_cas_strong(delete_node_ptr_id, (uint8_t *) next_ptr, (uint8_t *) new_node_ptr,
                                    sizeof(struct hm_ptr), &info->success,
                                    true, real_sess_i);
      info->state = HM_MARKED_POTENTIAL_SUCCESS;
      break;
    case HM_MARKED_POTENTIAL_SUCCESS:
      poll_a_req_blocking(real_sess_i, info->last_req_id);
      if (CLIENT_ASSERTIONS) assert(!next_ptr->marked);
      if (info->success) { // attempt to link node out of the list
        memcpy(new_curr_ptr, curr_ptr, sizeof(struct hm_ptr));
        if (CLIENT_ASSERTIONS) {
          assert(!curr_ptr->marked);
          assert(new_curr_ptr->next_key_id == info->owned_key);
        }
        new_curr_ptr->counter++;
        new_curr_ptr->next_key_id = next_ptr->next_key_id;

        info->last_req_id =
          (uint32_t) async_cas_strong(info->prev_ptr, (uint8_t *) curr_ptr, (uint8_t *) new_curr_ptr,
                                      sizeof(struct hm_ptr), &info->success,
                                      true, real_sess_i);
        info->state = HM_POTENTIAL_SUCCESS;
      }
      else {
        hm_transition_to_searching(info);
        new_node_ptr->my_key_id = info->owned_key_ptr;
      }
      break;
      // You must ensure that the second CAS went through, before reusing the node,
      // otherwise the old list may still point to it until after(!) you unmark it
    case HM_POTENTIAL_SUCCESS:
    case HM_CL_AFTER_SEARCH: // return here after a search that is used to clean up
      poll_a_req_blocking(real_sess_i, info->last_req_id);
      if (info->success || info->state == HM_CL_AFTER_SEARCH) {
        if (CLIENT_ASSERTIONS) {
          if (info->state == HM_CL_AFTER_SEARCH) {
            assert(!info->search_found);
            assert(curr_ptr->next_key_id != info->owned_key_ptr);
          }
        }
        info->ins_or_del_state = HM_INSERTING;
        info->state = HM_INIT;
        c_stats[t_id].microbench_pops++;
        info->delete_num++;
        //my_printf(yellow, "Cl %u/%u D %u/%u\n", t_id,
        //              info->glob_sess_i, info->owned_key_ptr, info->list_id);
        if (!HM_NO_CONFLICT) {
          info->list_id = *queue_id_cntr;
          MOD_INCR(*queue_id_cntr, HM_LISTS_NUM);
        }

      }
      else {
        info->ins_or_del_state = HM_CLEAN_UP; // so that search knows where to return
        // this search makes sure that the 2nd CAS to curr_ptr goes through,
        // because it loops until all marked nodes are removed!!
        hm_transition_to_searching(info);
        new_node_ptr->my_key_id = info->owned_key_ptr;
      }
      break;
    default: if (CLIENT_ASSERTIONS) assert(false);
  }

}


// High-level State Machine for the Harris & Michael lists
static inline void hm_insert_delete_multi_session(uint16_t t_id)
{
  if (machine_id == 0 && t_id == 0) {
    hm_init_print();
    hm_set_up_lists(t_id);
  }
  else hm_wait_for_init(t_id);

  assert(sizeof(struct hm_ptr) <= VALUE_SIZE);
  assert(sizeof(struct hm_node) <= VALUE_SIZE);
  assert(NUM_OF_RMW_KEYS > LAST_HM_NODE_PTR);
  assert(HM_WRITES_NUM > 0);
  assert(HM_FREE_NODE_NUM >= GLOBAL_SESSION_NUM);
  uint16_t s_i = 0, sess_offset = (uint16_t) (t_id * SESSIONS_PER_CLIENT);
  assert(sess_offset + SESSIONS_PER_CLIENT <= SESSIONS_PER_MACHINE);
  struct hm_sess_info *info = calloc(SESSIONS_PER_CLIENT, sizeof(struct hm_sess_info));


  uint32_t key_offset = (uint32_t) (HM_FREE_NODE_KEY_OFFSET + (HM_WRITES_NUM * (SESSIONS_PER_MACHINE * machine_id)));
  uint32_t first_key = key_offset + (sess_offset * HM_WRITES_NUM);

  uint32_t last_key = key_offset + ((sess_offset + (SESSIONS_PER_CLIENT -1)) * HM_WRITES_NUM);
  uint16_t min_sess = sess_offset, max_sess = (uint16_t) (sess_offset + (SESSIONS_PER_CLIENT -1));
  uint16_t min_wrkr = (uint16_t) (min_sess / SESSIONS_PER_THREAD),
    max_wrkr = (uint16_t) (max_sess / SESSIONS_PER_THREAD);

  uint32_t queue_id_cntr = ((machine_id * 100) + last_key) % HM_LISTS_NUM;



  printf("Client %u: sessions %u to %u, workers %u to %u, keys %u to %u, globally max key %u \n",
         t_id, min_sess, max_sess, min_wrkr, max_wrkr,
         first_key, last_key, (HM_INIT_DONE_FLAG_KEY - 1));

  for (s_i = 0; s_i < SESSIONS_PER_CLIENT; s_i++) {
    info[s_i].state = HM_INIT;
    info[s_i].ins_or_del_state = HM_INSERTING;
    info[s_i].real_sess_i = sess_offset + s_i;
    info[s_i].glob_sess_i = (uint16_t) (SESSIONS_PER_MACHINE * machine_id + info[s_i].real_sess_i);
    info[s_i].owned_key = (uint32_t) (HM_FREE_NODE_KEY_OFFSET + (info[s_i].glob_sess_i * HM_WRITES_NUM));
    info[s_i].owned_key_ptr = hm_get_node_ptr_key_id(info[s_i].owned_key);

    if (CLIENT_ASSERTIONS) assert(info[s_i].real_sess_i < SESSIONS_PER_MACHINE);
    info[s_i].new_node = calloc(HM_WRITES_NUM, (sizeof(struct hm_node)));
    info[s_i].list_id = info[s_i].glob_sess_i;
    assert(info[s_i].list_id < HM_LISTS_NUM);
  }

  s_i = 0;
  while(true) {
    switch (info[s_i].ins_or_del_state) {
      case HM_SEARCHING_INS:
      case HM_SEARCHING_DEL:
      case HM_SEARCHING_CL:
        hm_search_state_machine(&info[s_i],  t_id);
        break;
      case HM_INSERTING:
        hm_insert_state_machine(&info[s_i],  t_id);
        break;
      case HM_DELETING:
        hm_delete_state_machine(&info[s_i], &queue_id_cntr, t_id);
        break;
      default: if (CLIENT_ASSERTIONS) assert(false);
    }
    MOD_INCR(s_i, SESSIONS_PER_CLIENT);
  }
}


/* ------------------------------------------------------------------------------------------------------------------- */
/*------------------------------PRODUCER CONSUMER LIST------------------------------------------------------------------------------*/
/* ------------------------------------------------------------------------------------------------------------------- */
#define PCINSERTING 0
#define PCDELETING 1
#define PCCLEAN_UP 2
#define PCSEARCHING_INS 3
#define PCSEARCHING_DEL 4
#define PC_SEARCHING_CL 5
//
//
////PC_STATES
#define PC_TRY_TO_ACQUIRE 0
#define PC_POLL_LOCAL_STORE 1
#define PC_TRY_INSERT 2
#define PC_TRY_DELETE 12
#define PC_READ_CURR_PTR_AGAIN 3
#define PC_POTENTIAL_SUCCESS 4
#define PC_SEARCH_INNER_LOOP_AFTER_COPYING_PTRS 5 // this state is for when traversing the list, where the curr_ptr is copied from next ptr
#define PC_MARKED_POTENTIAL_SUCCESS 6 // after the delete attempts to mark a node
#define PC_CL_AFTER_SEARCH 2
#define PC_READ_FIRST_NODE 3
//
//
#define PC_MAX_WAIT_LOOPS 100
#define PC_NODE_SIZE (VALUE_SIZE - 12)
#define PC_PTR_SIZE 12;
//
//
//// KEY ALLOCATION-- SIZES & OFFSETS
#define PC_FLAGS_NUM (GLOBAL_SESSION_NUM)
#define PC_NODE_NUM (GLOBAL_SESSION_NUM * PC_WRITES_NUM)

//
#define PC_FLAG_KEY_ID_OFFSET (NUM_OF_RMW_KEYS) // Flag used by session 0 of machine 0
#define PC_NODE_OFFSET (NUM_OF_RMW_KEYS + PC_FLAGS_NUM)
#define LAST_PC_NODE_PTR (PC_NODE_OFFSET + PC_NODE_NUM)
//


/* ----Key allocation----
 * NUM_OF_RMW_KEYS -> PC_FLAGS_NUM                 : flags
 * PC_NODE_OFFSET -> PC_NODE_OFFSET + PC_NODE_NUM   : nodes
 * */

struct pc_ptr {
  uint8_t value[PC_NODE_SIZE];
  uint32_t owner; // owner's global_sess_id
  uint64_t counter; // times released
};

struct pc_node {
  uint8_t value[PC_NODE_SIZE];
  uint32_t owner; // owner's global_sess_id
  uint64_t times_written; //
};

struct pc_sess_info {
  uint8_t state;
  uint16_t real_sess_i;
  uint16_t glob_sess_i;
  uint16_t session_to_acquire_from; // the glob_sess
  uint32_t last_req_id;
  uint32_t flag_to_acquire;
  uint32_t flag_to_release;
  uint32_t w_key; // they key of the pc_node to be written
  uint32_t r_key; // they key of the pc_node to be read

  uint64_t acq_num;
  uint64_t rel_num;

  struct pc_node *node_to_read;
  struct pc_node *node_to_write;
  struct pc_ptr rel_flag;
  struct pc_ptr acq_flag;

};


static inline void pc_init_flags(struct pc_sess_info *info_,
                                 uint16_t t_id)
{
  for (uint32_t s_i = 0; s_i < SESSIONS_PER_CLIENT; s_i++) {
    struct pc_sess_info *info = &info_[s_i];
    struct pc_node *w_node = info->node_to_write;
    struct pc_ptr *rel_flag = &info->rel_flag;


    w_node[0].owner = info->glob_sess_i;
    w_node[0].times_written = 1;
    for (uint8_t i = 0; i < PC_WRITES_NUM; i++)
      async_write_strong(info->w_key + i, (uint8_t*) w_node,
                        sizeof(struct pc_node), info->real_sess_i);

    rel_flag->counter = 1;
    rel_flag->owner = info->glob_sess_i;
    async_release_strong(info->flag_to_release, (uint8_t *) rel_flag,
                         (sizeof(struct pc_ptr)), info->real_sess_i);
    info->rel_num = 1;

  }
}

// Per-sessionProducer Consumer FSM
static inline void pc_per_sess_state_machine(struct pc_sess_info *info_,
                                             uint16_t t_id)
{
  uint16_t s_i = 0;

  while(true) {

    struct pc_sess_info *info = &info_[s_i];
    struct pc_node *w_node = info->node_to_write;
    struct pc_node *r_node = info->node_to_read;
    struct pc_ptr *acq_flag = &info->acq_flag;
    struct pc_ptr *rel_flag = &info->rel_flag;

    uint16_t real_sess_i = info->real_sess_i;
    uint32_t delete_node_ptr_id;
    if (CLIENT_ASSERTIONS) assert(real_sess_i < SESSIONS_PER_MACHINE);

    switch (info->state) {
      case PC_POLL_LOCAL_STORE:
        info->last_req_id = (uint32_t)  async_read_strong(info->flag_to_acquire, (uint8_t *) acq_flag,
                                                          (sizeof(struct pc_ptr)), real_sess_i);
        info->state = PC_TRY_TO_ACQUIRE;
        break;
      case PC_TRY_TO_ACQUIRE:
        if (!PC_IDEAL) {
          poll_a_req_blocking(real_sess_i, info->last_req_id);
          if (acq_flag->counter != info->acq_num + 1) {
            info->state = PC_POLL_LOCAL_STORE;
            break;
          }
        }
        // the flag is seen raised, do the acquire to ensure correctness and issue reads, writes and the release
        async_acquire_strong(info->flag_to_acquire, (uint8_t *) acq_flag,
                             (sizeof(struct pc_ptr)), real_sess_i);


//        printf("%lu/%lu \n", w_node[0].times_written, info->rel_num);
        if (CLIENT_ASSERTIONS && !PC_IDEAL) {
          assert(w_node[0].times_written == info->rel_num);
          assert(w_node[0].owner == info->glob_sess_i);
          if (info->acq_num > 0) {
            assert(r_node[0].owner == info->session_to_acquire_from);
            assert(r_node[0].times_written == info->acq_num);
          }
        }
        w_node[0].times_written++;
        for (uint8_t i = 0; i < PC_WRITES_NUM; i++) {
          async_read_strong(info->r_key + i, (uint8_t *) r_node,
                            sizeof(struct pc_node), real_sess_i);

          async_write_strong(info->w_key + i, (uint8_t *) w_node,
                            sizeof(struct pc_node), real_sess_i);
        }

        rel_flag->counter++;
//        printf("polled flag %lu, releasing flag %lu\n",
//               info->acq_num + 1, rel_flag->counter);
        assert(rel_flag->counter == info->rel_num + 1);
        async_release_strong(info->flag_to_release, (uint8_t *) rel_flag,
                             (sizeof(struct pc_ptr)), real_sess_i);

        info->acq_num++;
        info->rel_num++;
        c_stats[t_id].microbench_pushes++;
        c_stats[t_id].microbench_pops++;
        if (PC_IDEAL) info->state = PC_TRY_TO_ACQUIRE;
        else info->state = PC_POLL_LOCAL_STORE;
        break;
      default:
        if (CLIENT_ASSERTIONS) assert(false);
    }
    MOD_INCR(s_i, SESSIONS_PER_CLIENT);
  }

}


// High-level State Machine for the Producer Consumer benchmark
static inline void pc_multi_session(uint16_t t_id)
{


  assert(sizeof(struct pc_ptr) <= VALUE_SIZE);
  assert(sizeof(struct pc_node) == VALUE_SIZE);
  assert(NUM_OF_RMW_KEYS == PC_FLAG_KEY_ID_OFFSET);
  assert(PC_WRITES_NUM > 0);
  assert(PC_NODE_NUM >= GLOBAL_SESSION_NUM);


  uint16_t s_i = 0, sess_offset = (uint16_t) (t_id * SESSIONS_PER_CLIENT);
  assert(sess_offset + SESSIONS_PER_CLIENT <= SESSIONS_PER_MACHINE);
  struct pc_sess_info *info = calloc(SESSIONS_PER_CLIENT, sizeof(struct pc_sess_info));


//  uint32_t key_offset = (uint32_t) (HM_FREE_NODE_KEY_OFFSET + (HM_WRITES_NUM * (SESSIONS_PER_MACHINE * machine_id)));
//  uint32_t first_key = key_offset + (sess_offset * HM_WRITES_NUM);
//
//  uint32_t last_key = key_offset + ((sess_offset + (SESSIONS_PER_CLIENT -1)) * HM_WRITES_NUM);
//  uint16_t min_sess = sess_offset, max_sess = (uint16_t) (sess_offset + (SESSIONS_PER_CLIENT -1));
//  uint16_t min_wrkr = (uint16_t) (min_sess / SESSIONS_PER_THREAD),
//    max_wrkr = (uint16_t) (max_sess / SESSIONS_PER_THREAD);
//
//  uint32_t queue_id_cntr = ((machine_id * 100) + last_key) % HM_LISTS_NUM;
//
//
//
//  printf("Client %u: sessions %u to %u, workers %u to %u, keys %u to %u, globally max key %u \n",
//         t_id, min_sess, max_sess, min_wrkr, max_wrkr,
//         first_key, last_key, (HM_INIT_DONE_FLAG_KEY - 1));

  for (s_i = 0; s_i < SESSIONS_PER_CLIENT; s_i++) {
    info[s_i].state = PC_TRY_TO_ACQUIRE;
    info[s_i].real_sess_i = sess_offset + s_i;
    info[s_i].glob_sess_i = (uint16_t) (SESSIONS_PER_MACHINE * machine_id + info[s_i].real_sess_i);
    info[s_i].session_to_acquire_from = (uint16_t) ((GLOBAL_SESSION_NUM + info[s_i].glob_sess_i - SESSIONS_PER_MACHINE) % GLOBAL_SESSION_NUM);
    info[s_i].flag_to_acquire = PC_FLAG_KEY_ID_OFFSET + info[s_i].session_to_acquire_from;
    info[s_i].flag_to_release = PC_FLAG_KEY_ID_OFFSET + info[s_i].glob_sess_i;
    info[s_i].r_key = (uint32_t) (PC_NODE_OFFSET + (info[s_i].session_to_acquire_from * PC_WRITES_NUM));
    info[s_i].w_key = (uint32_t) (PC_NODE_OFFSET + (info[s_i].glob_sess_i * PC_WRITES_NUM));
    //my_printf(green, "%u/%u acquires from %u, flag/key %u/%u\n",
    //             t_id, info[s_i].glob_sess_i,  info[s_i].session_to_acquire_from, info[s_i].flag_to_acquire, info[s_i].r_key);

    if (CLIENT_ASSERTIONS) assert(info[s_i].real_sess_i < SESSIONS_PER_MACHINE);
    info[s_i].node_to_write = calloc(PC_WRITES_NUM, (sizeof(struct pc_node)));
    info[s_i].node_to_read = calloc(PC_WRITES_NUM, (sizeof(struct pc_node)));
    info[s_i].node_to_write[0].owner = info[s_i].glob_sess_i;
  }

  if (machine_id == 0) pc_init_flags(info, t_id);
  pc_per_sess_state_machine(info, t_id);
}




/* --------------------------------------------------------------------------------------
 * ----------------------------------CLIENT THREAD----------------------------------------
 * --------------------------------------------------------------------------------------*/

void *client(void *arg) {
  struct thread_params params = *(struct thread_params *) arg;
  uint16_t t_id = (uint16_t) params.id;
  trace_t *trace;
  my_printf(green, "Client %u reached loop \n", t_id);
  while (true) {
    switch (CLIENT_MODE) {
      case CLIENT_USE_TRACE:
        trace = trace_init(t_id);
        send_reqs_from_trace(trace, t_id);
        break;
      case CLIENT_UI:
        user_interface();
        break;
      case BLOCKING_TEST_CASE:
        rel_acq_circular_blocking();
        break;
      case ASYNC_TEST_CASE:
        rel_acq_circular_async();
        break;
      case TREIBER_BLOCKING:
        treiber_push_pop_blocking();
        break;
      case TREIBER_DEBUG:
        treiber_push_pull_multi_session_dbg(t_id);
        break;
      case TREIBER_ASYNC:
        treiber_push_pull_multi_session(t_id);
        break;
      case MSQ_ASYNC:
        ms_enqueue_dequeue_multi_session(t_id);
        break;
      case HML_ASYNC:
        hm_insert_delete_multi_session(t_id);
        break;
      case PRODUCER_CONSUMER:
        pc_multi_session(t_id);
      default:
        if (CLIENT_ASSERTIONS) assert(false);
    }
  }
}