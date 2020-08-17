//
// Created by vasilis on 16/07/20.
//

#ifndef ODYSSEY_FIFO_H
#define ODYSSEY_FIFO_H


#include "top.h"

typedef struct per_fifo_slot_meta {
  uint16_t coalesce_num;
  uint32_t byte_size;
  uint32_t resp_size;
  uint32_t backward_ptr; // ptr to a different fifo
  uint8_t rm_id; // machine to send this message, if unicast
} slot_meta_t;

typedef struct fifo {
  void *fifo;
  slot_meta_t *slot_meta;
  void* ctx;
  uint32_t push_ptr;
  uint32_t pull_ptr;
  uint32_t max_size; //in slots
  uint32_t max_byte_size;
  uint32_t capacity;
  uint32_t net_capacity;
  uint32_t slot_size;
  uint16_t mes_header;
} fifo_t;

static inline uint32_t mod_decr(uint32_t x, uint32_t N)
{
  return ((N + x - 1) % N);
}

static inline uint32_t mod_incr(uint32_t x, uint32_t N)
{
  return ((x + 1) % N);
}
/*---------------------------------------------------------------------
 * -----------------------FIFO Handles------------------------------
 * ---------------------------------------------------------------------*/

static inline void check_fifo(fifo_t *fifo)
{
  if (ENABLE_ASSERTIONS) {
    assert(fifo != NULL);
    assert(fifo->fifo != NULL);
    assert(fifo->max_size > 0);
    assert(fifo->slot_size > 0);
    assert(fifo->capacity <= fifo->max_size);
    assert(fifo->slot_size * fifo->max_size == fifo->max_byte_size);
    assert(fifo->push_ptr < fifo->max_size);
    assert(fifo->pull_ptr < fifo->max_size);
  }
}

static inline void check_slot_and_slot_meta(fifo_t *fifo, uint32_t slot_no)
{
  if (ENABLE_ASSERTIONS) {
    assert(slot_no < fifo->max_size);
    assert(fifo->slot_meta != NULL);
  }
}

static inline fifo_t *fifo_constructor(uint32_t max_size,
                                       uint32_t slot_size,
                                       bool alloc_slot_meta,
                                       uint16_t mes_header,
                                       uint32_t fifo_num)
{
  fifo_t *fifo = calloc(fifo_num, (sizeof(fifo_t)));
  for (int fifo_i = 0; fifo_i < fifo_num; ++fifo_i) {
    fifo[fifo_i].max_size = max_size;
    fifo[fifo_i].slot_size = slot_size;
    fifo[fifo_i].max_byte_size = max_size * slot_size;
    fifo[fifo_i].fifo = calloc(fifo[fifo_i].max_byte_size, sizeof(uint8_t));

    if (alloc_slot_meta) {
      fifo[fifo_i].slot_meta = calloc(max_size, sizeof(slot_meta_t));
      fifo[fifo_i].slot_meta[0].byte_size = mes_header;
    }
    fifo[fifo_i].mes_header = mes_header;
    check_fifo(&fifo[fifo_i]);
  }
  //fifo->max_size = max_size;
  //fifo->slot_size = slot_size;
  //fifo->max_byte_size = max_size * slot_size;
  //fifo->fifo = calloc(fifo->max_byte_size, sizeof(uint8_t));
  //
  //if (alloc_slot_meta) {
  //  fifo->slot_meta = calloc(max_size, sizeof(slot_meta_t));
  //  fifo->slot_meta[0].byte_size = mes_header;
  //}
  //fifo->mes_header = mes_header;
  //check_fifo(fifo);
  return fifo;
}

/// FIFO SLOT-- this is private
static inline void* get_fifo_slot_private(fifo_t *fifo, uint32_t slot, uint32_t offset)
{
  check_fifo(fifo);
  if (ENABLE_ASSERTIONS) {
    assert(slot < fifo->max_size);
    assert((fifo->slot_size * slot) + offset < fifo->max_byte_size);
  }
  return fifo->fifo + (fifo->slot_size * slot) + offset;
}


static inline void* get_fifo_push_slot(fifo_t *fifo)
{
  return get_fifo_slot_private(fifo, fifo->push_ptr, 0);
}

static inline void* get_fifo_pull_slot(fifo_t *fifo)
{
  return get_fifo_slot_private(fifo, fifo->pull_ptr, 0);
}

static inline void* get_fifo_slot(fifo_t *fifo, uint32_t slot_no)
{
  return get_fifo_slot_private(fifo, slot_no, 0);
}

static inline void* get_fifo_slot_mod(fifo_t *fifo, uint32_t slot_no)
{
  return get_fifo_slot_private(fifo, slot_no % fifo->max_size, 0);
}


static inline void* get_fifo_push_slot_with_offset(fifo_t *fifo, uint32_t offset)
{
  return get_fifo_slot_private(fifo, fifo->push_ptr, offset);
}

static inline void* get_fifo_pull_slot_with_offset(fifo_t *fifo, uint32_t offset)
{
  return get_fifo_slot_private(fifo, fifo->pull_ptr, offset);
}

static inline void* get_fifo_specific_slot_with_offset(fifo_t *fifo, uint32_t slot_no,
                                                       uint32_t offset)
{
  return get_fifo_slot_private(fifo, slot_no, offset);
}

static inline void* get_fifo_prev_slot(fifo_t *fifo, uint32_t slot_no)
{
  slot_no = mod_decr(slot_no, fifo->max_size);
  return get_fifo_slot_private(fifo, slot_no, 0);
}

static inline void* get_fifo_pull_prev_slot(fifo_t *fifo)
{
  return get_fifo_prev_slot(fifo, fifo->pull_ptr);
}

static inline void* get_fifo_push_prev_slot(fifo_t *fifo)
{
  return get_fifo_prev_slot(fifo, fifo->push_ptr);
}




/// INCREMENT PUSH PULL
static inline void fifo_incr_push_ptr(fifo_t *fifo)
{
  check_fifo(fifo);
  fifo->push_ptr = mod_incr(fifo->push_ptr, fifo->max_size);
  check_fifo(fifo);
}

static inline void fifo_incr_pull_ptr(fifo_t *fifo)
{
  check_fifo(fifo);
  fifo->pull_ptr = mod_incr(fifo->pull_ptr, fifo->max_size);
  check_fifo(fifo);
}

// GET SLOT_META

static inline slot_meta_t* get_fifo_slot_meta(fifo_t *fifo, uint32_t slot_no)
{
  check_fifo(fifo);
  check_slot_and_slot_meta(fifo, slot_no);
  return &fifo->slot_meta[slot_no];
}

static inline slot_meta_t* get_fifo_slot_meta_push(fifo_t *fifo)
{
  return get_fifo_slot_meta(fifo, fifo->push_ptr);
}

static inline slot_meta_t* get_fifo_slot_meta_pull(fifo_t *fifo)
{
  return get_fifo_slot_meta(fifo, fifo->pull_ptr);
}

// GET/ SET BACKWARDS_PTRS
static inline uint32_t fifo_get_backward_ptr(fifo_t *fifo, uint32_t slot_no)
{
  check_fifo(fifo);
  check_slot_and_slot_meta(fifo, slot_no);

  return fifo->slot_meta[slot_no].backward_ptr;
}

static inline uint32_t fifo_get_push_backward_ptr(fifo_t *fifo)
{
  return fifo_get_backward_ptr(fifo, fifo->push_ptr);
}

static inline uint32_t fifo_get_pull_backward_ptr(fifo_t *fifo)
{
  return fifo_get_backward_ptr(fifo, fifo->pull_ptr);
}

static inline void fifo_set_backward_ptr(fifo_t *fifo, uint32_t slot_no, uint32_t new_ptr)
{
  check_fifo(fifo);
  if (ENABLE_ASSERTIONS) {
    assert(slot_no < fifo->max_size);
    assert(fifo->slot_meta != NULL);
  }
  fifo->slot_meta[slot_no].backward_ptr = new_ptr;
}

static inline void fifo_set_push_backward_ptr(fifo_t *fifo, uint32_t new_ptr)
{
  fifo_set_backward_ptr(fifo, fifo->push_ptr, new_ptr);
}

static inline void fifo_set_pull_backward_ptr(fifo_t *fifo, uint32_t new_ptr)
{
  fifo_set_backward_ptr(fifo, fifo->pull_ptr, new_ptr);
}


///
static inline void fifo_incr_capacity(fifo_t *fifo)
{
  check_fifo(fifo);
  fifo->capacity++;
  check_fifo(fifo);
}

static inline void fifo_decrem_capacity(fifo_t *fifo)
{
  check_fifo(fifo);
  fifo->capacity--;
  check_fifo(fifo);
}

static inline void fifo_decrease_capacity(fifo_t *fifo, uint32_t reduce_num)
{
  check_fifo(fifo);
  if (ENABLE_ASSERTIONS) assert(fifo->capacity >= reduce_num);
  fifo->capacity -= reduce_num;
  check_fifo(fifo);
}

///
static inline void fifo_incr_net_capacity(fifo_t *fifo)
{
  check_fifo(fifo);
  fifo->net_capacity++;
}


///
// Set up a fresh read message to coalesce requests -- Proposes, reads, acquires
static inline void reset_fifo_slot(fifo_t *send_fifo)
{
  check_fifo(send_fifo);
  fifo_incr_push_ptr(send_fifo);
  slot_meta_t *slot_meta = get_fifo_slot_meta_push(send_fifo);

  slot_meta->byte_size = (uint16_t) send_fifo->mes_header;
  slot_meta->resp_size = 0;
  slot_meta->coalesce_num = 0;

}


static inline void* get_send_fifo_ptr(fifo_t* send_fifo,
                                      uint32_t new_size,
                                      uint32_t resp_size,
                                      bool create_new_mes,
                                      uint16_t t_id)
{
  check_fifo(send_fifo);
  slot_meta_t *slot_meta = get_fifo_slot_meta_push(send_fifo);
  slot_meta->resp_size += resp_size;


  bool new_message_because_of_rep = slot_meta->resp_size > MTU;
  bool new_message_because_of_size = (slot_meta->byte_size + new_size) > send_fifo->slot_size;


  bool is_empty = slot_meta->coalesce_num == 0;
  bool new_message = new_message_because_of_rep ||
                     new_message_because_of_size ||
                    (create_new_mes && !is_empty);

  //printf("new_message %d, slot size %u, is_empty %d, input_new_mes %d \n",
  //       new_message, send_fifo->slot_size, is_empty, create_new_mes);
  if (new_message) {
    if (ENABLE_ASSERTIONS) assert(!is_empty);
    reset_fifo_slot(send_fifo);
    slot_meta = get_fifo_slot_meta_push(send_fifo);
  }

  slot_meta->coalesce_num++;
  uint32_t inside_r_ptr = slot_meta->byte_size;
  slot_meta->byte_size += new_size;
  if (ENABLE_ASSERTIONS) {
    assert(slot_meta->byte_size <= send_fifo->slot_size);
    assert(slot_meta->byte_size <= MTU);
  }
  check_fifo(send_fifo);
  return get_fifo_push_slot_with_offset(send_fifo, inside_r_ptr);
}


//
static inline void fifo_send_from_pull_slot(fifo_t* send_fifo)
{
  uint16_t coalesce_num = get_fifo_slot_meta_pull(send_fifo)->coalesce_num;
  send_fifo->net_capacity -= coalesce_num;
  if (send_fifo->net_capacity == 0)
    reset_fifo_slot(send_fifo);
  fifo_decrem_capacity(send_fifo);
  fifo_incr_pull_ptr(send_fifo);
}


/*------------------------------------------------------
 * ----------------BUFFER MIRRORING------------------------
 * ----------------------------------------------------*/

/// Sometimes it's hard to infer or even send credits.
/// For example a zk-follower infers the credits to send writes to the leader
// by examining the incoming commits.
/// In this case it is useful to mirror the buffer of remote machines,
/// to infer their buffer availability



// Generic function to mirror buffer spaces--used when elements are added
static inline void add_to_the_mirrored_buffer(struct fifo *mirror_buf, uint8_t coalesce_num,
                                              uint16_t number_of_fifos,
                                              uint32_t max_size,
                                              quorum_info_t *q_info)
{
  for (uint16_t i = 0; i < number_of_fifos; i++) {
    uint32_t push_ptr = mirror_buf[i].push_ptr;
    uint16_t *fifo = (uint16_t *) mirror_buf[i].fifo;
    fifo[push_ptr] = (uint16_t) coalesce_num;
    fifo_incr_push_ptr(&mirror_buf[i]);
    mirror_buf[i].capacity++;
    if (mirror_buf[i].capacity > max_size) {
      // this may noy be an error if there is a failure
      assert(q_info != NULL);
      assert(q_info->missing_num > 0);
    }
    //if (ENABLE_ASSERTIONS) assert(mirror_buf[i].capacity <= max_size);
  }
}

// Generic function to mirror buffer spaces--used when elements are removed
static inline uint16_t remove_from_the_mirrored_buffer(struct fifo *mirror_buf_, uint16_t remove_num,
                                                       uint16_t t_id, uint8_t fifo_id, uint32_t max_size)
{
  fifo_t *mirror_buf = &mirror_buf_[fifo_id];
  uint16_t *fifo = (uint16_t *) mirror_buf->fifo;
  uint16_t new_credits = 0;
  if (ENABLE_ASSERTIONS && mirror_buf->capacity == 0) {
    my_printf(red, "REMOVING FROM MIRROR BUFF, WITH ZERO CAPACITY: "
                "remove_num %u, mirror_buf->pull_ptr %u fifo_id %u, total size %u \n",
              remove_num, mirror_buf->pull_ptr, fifo_id, mirror_buf->max_size);
    assert(false);
  }
  while (remove_num > 0) {
    uint32_t pull_ptr = mirror_buf->pull_ptr;
    if (fifo[pull_ptr] <= remove_num) {
      remove_num -= fifo[pull_ptr];
      fifo_incr_pull_ptr(mirror_buf);
      if (ENABLE_ASSERTIONS && mirror_buf->capacity == 0) {
        my_printf(red, "REMOVING FROM MIRROR BUFF, WITH ZERO CAPACITY: "
                    "remove_num %u, mirror_buf->pull_ptr %u fifo_id %u  \n",
                  remove_num, mirror_buf->pull_ptr, fifo_id);
        assert(false);
      }
      mirror_buf->capacity--;
      new_credits++;
    }
    else {
      fifo[pull_ptr] -= remove_num;
      remove_num = 0;
    }
  }
  return new_credits;
}


#endif //ODYSSEY_FIFO_H
