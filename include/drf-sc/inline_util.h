#ifndef INLINE_UTILS_H
#define INLINE_UTILS_H

#include "cache.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
#include <optik_mod.h>

/* ---------------------------------------------------------------------------
------------------------------UTILITY --------------------------------------
---------------------------------------------------------------------------*/

// swap 2 pointerss
static inline void swap_pointers(void** ptr_1, void** ptr_2)
{
	void* tmp = *ptr_1;
	*ptr_1 = *ptr_2;
	*ptr_2 = tmp;
}

// Swap 3 pointers in a cirular fashion
static inline void circulate_pointers(void** ptr_1, void** ptr_2, void** ptr_3)
{
	void* tmp = *ptr_1;
	*ptr_1 = *ptr_2;
	*ptr_2 = *ptr_3;
	*ptr_3 = tmp;
}

// Check whether 2 key hashes are equal
static inline bool keys_are_equal(struct cache_key* key1, struct cache_key* key2) {
	return (key1->bkt    == key2->bkt &&
			key1->server == key2->server &&
			key1->tag    == key2->tag) ? true : false;
}

// Compares two timestamps, returns SMALLER if ts1 < ts2
static inline enum ts_compare compare_ts(struct ts_tuple *ts1, struct ts_tuple *ts2)
{
  if ((*(uint32_t *)ts1->version == *(uint32_t *)ts2->version) &&
      (ts1->m_id == ts2->m_id))
    return EQUAL;
  else if ((*(uint32_t *)ts1->version < *(uint32_t *)ts2->version) ||
          ((*(uint32_t *)ts1->version == *(uint32_t *)ts2->version) &&
           (ts1->m_id < ts2->m_id)))
    return SMALLER;
  else if  ((*(uint32_t *)ts1->version > *(uint32_t *)ts2->version) ||
           ((*(uint32_t *)ts1->version == *(uint32_t *)ts2->version)) &&
            (ts1->m_id > ts2->m_id))
    return GREATER;

  return ERROR;


}


// Check whether 2 keys (including the metadata) are equal
static inline uint8_t keys_and_meta_are_equal(struct cache_key* key1, struct cache_key* key2) {
	return (key1->bkt    == key2->bkt &&
			key1->server == key2->server &&
			key1->tag    == key2->tag &&
			key1->meta.version == key2->meta.version) ? 1 : 0;
}

// A condition to be used to trigger periodic (but rare) measurements
static inline bool trigger_measurement(uint16_t local_client_id)
{
	return t_stats[local_client_id].cache_hits_per_thread % K_32 > 0 &&
		   t_stats[local_client_id].cache_hits_per_thread % K_32 <= CACHE_BATCH_SIZE &&
		   local_client_id == 0 && machine_id == MACHINE_NUM -1;
}

// Poll for the local reqs completion to measure a local req's latency
static inline void poll_local_req_for_latency_measurement(struct latency_flags* latency_info, struct timespec* start,
														  struct local_latency* local_measure)
{

	if ((MEASURE_LATENCY == 1) && ((latency_info->measured_req_flag) == LOCAL_REQ) && ((local_measure->local_latency_start_polling) == 1))
		if (*(local_measure->flag_to_poll) == 0) {
			struct timespec end;
			clock_gettime(CLOCK_MONOTONIC, &end);
			int useconds = ((end.tv_sec - start->tv_sec) * 1000000) +
						   ((end.tv_nsec - start->tv_nsec) / 1000);
			if (ENABLE_ASSERTIONS) assert(useconds > 0);
      //			yellow_printf("Latency of a local req, region %d, flag ptr %llu: %d us\n",
      //										local_measure->measured_local_region, local_measure->flag_to_poll, useconds);
			bookkeep_latency(useconds, LOCAL_REQ);
			latency_info->measured_req_flag = NO_REQ;
			local_measure->measured_local_region = -1;
			local_measure->local_latency_start_polling = 0;
			local_measure->flag_to_poll = NULL;

		}
}

static inline void report_remote_latency(struct latency_flags* latency_info, uint16_t prev_rem_req_i,
										 struct ibv_wc* wc, struct timespec* start)
{
	uint16_t i;
	for (i = 0; i < prev_rem_req_i; i++) {
		//	 printf("Looking for the req\n" );
		if (wc[i].imm_data == REMOTE_LATENCY_MARK) {
			struct timespec end;
			clock_gettime(CLOCK_MONOTONIC, &end);
			int useconds = ((end.tv_sec - start->tv_sec) * 1000000) +
						   ((end.tv_nsec - start->tv_nsec) / 1000);  //(end.tv_nsec - start->tv_nsec) / 1000;
			if (ENABLE_ASSERTIONS) assert(useconds > 0);
			//		printf("Latency of a Remote r_rep %u us\n", useconds);
			bookkeep_latency(useconds, REMOTE_REQ);
			(latency_info->measured_req_flag) = NO_REQ;
			break;
		}
	}
}
/* ---------------------------------------------------------------------------
//------------------------------ ABD GENERIC -----------------------------
//---------------------------------------------------------------------------*/
// Post Receives for acknowledgements
static inline void post_recvs_with_recv_info(struct recv_info *recv, uint32_t recv_num)
{
  if (recv_num == 0) return;
  uint16_t j;
  struct ibv_recv_wr *bad_recv_wr;
  for (j = 0; j < recv_num; j++) {
    recv->recv_sgl[j].addr = (uintptr_t) recv->buf + (recv->push_ptr * recv->slot_size);
//    printf("Posting a receive at push ptr %u at address %lu \n", recv->w_push_ptr, recv->recv_sgl[j].addr);
    MOD_ADD(recv->push_ptr, recv->buf_slots);
    recv->recv_wr[j].next = (j == recv_num - 1) ?
                            NULL : &recv->recv_wr[j + 1];
  }
  int ret = ibv_post_recv(recv->recv_qp, &recv->recv_wr[0], &bad_recv_wr);
  if (ENABLE_ASSERTIONS) CPE(ret, "ibv_post_recv error", ret);
}

/* Fill @wc with @num_comps comps from this @cq. Exit on error. */
static inline uint32_t poll_cq(struct ibv_cq *cq, int num_comps, struct ibv_wc *wc, uint8_t caller_flag)
{
  int comps = 0;
  uint32_t debug_cnt = 0;
  while(comps < num_comps) {
    if (ENABLE_ASSERTIONS && debug_cnt > M_256) {
      printf("Someone is stuck waiting for a completion %d / %d , type %u  \n", comps, num_comps, caller_flag );
      debug_cnt = 0;
    }
    int new_comps = ibv_poll_cq(cq, num_comps - comps, &wc[comps]);
    if(new_comps != 0) {
//			 printf("I see completions %d\n", new_comps);
      /* Ideally, we should check from comps -> new_comps - 1 */
      if(ENABLE_ASSERTIONS && wc[comps].status != 0) {
        fprintf(stderr, "Bad wc status %d\n", wc[comps].status);
        exit(0);
      }
      comps += new_comps;
    }
    if (ENABLE_ASSERTIONS) debug_cnt++;
  }
  return debug_cnt;
}

static inline void adaptive_inlining (uint32_t mes_size, struct ibv_send_wr *send_wr, uint16_t wr_num)
{
  int flag = mes_size < MAXIMUM_INLINE_SIZE ? IBV_SEND_INLINE : 0;
  for (uint16_t i = 0; i < wr_num; i++)
    send_wr[i].send_flags = flag;
}

// Convert a machine id to a "remote machine id"
static inline uint8_t  mid_to_rmid(uint8_t m_id)
{
  return m_id < machine_id ? m_id : (uint8_t)(m_id - 1);
}

// Convert a "remote machine id" to a machine id
static inline uint8_t  rmid_to_mid(uint8_t rm_id)
{
  return rm_id < machine_id ? rm_id : (uint8_t)(rm_id + 1);
}


static inline void print_q_info(struct quorum_info *q_info)
{
  yellow_printf("-----QUORUM INFO----- \n");
  green_printf("Active m_ids: \n");
  for (uint8_t i = 0; i < q_info->active_num; i++) {
    green_printf("%u) %u \n", i, q_info->active_ids[i]);
  }
  red_printf("Missing m_ids: \n");
  for (uint8_t i = 0; i < q_info->missing_num; i++) {
    red_printf("%u) %u \n", i, q_info->missing_ids[i]);
  }
  yellow_printf("Send vector : ");
  for (uint8_t i = 0; i < REM_MACH_NUM; i++) {
    yellow_printf("%d ", q_info->send_vector[i]);
  }
  yellow_printf("\n First rm_id: %u, Last rm_id: %u \n",
                q_info->first_active_rm_id, q_info->last_active_rm_id);
}

/* ---------------------------------------------------------------------------
//------------------------------ ABD DEBUGGING -----------------------------
//---------------------------------------------------------------------------*/
static inline void print_wrkr_stats (uint16_t t_id)
{
  green_printf("WORKER %u SENT MESSAGES \n", t_id);
  yellow_printf("Writes sent %ld/%ld \n", t_stats[t_id].writes_sent_mes_num, t_stats[t_id].writes_sent);
  yellow_printf("Acks sent %ld/%ld \n", t_stats[t_id].acks_sent_mes_num, t_stats[t_id].acks_sent);
  yellow_printf("Reads sent %ld/%ld \n", t_stats[t_id].reads_sent_mes_num, t_stats[t_id].reads_sent);
  yellow_printf("R_reps sent %ld/%ld \n", t_stats[t_id].r_reps_sent_mes_num, t_stats[t_id].r_reps_sent);
  green_printf("WORKER %u RECEIVED MESSAGES \n", t_id);
  //yellow_printf("Writes sent %ld/%ld \n", t_stats[t_id].writes_sent_mes_num, t_stats[t_id].writes_sent);
  //yellow_printf("Acks sent %ld/%ld \n", t_stats[t_id].acks_sent_mes_num, t_stats[t_id].acks_sent);
  yellow_printf("Reads received %ld/%ld \n", t_stats[t_id].received_reads_mes_num, t_stats[t_id].received_reads);
  yellow_printf("R_reps received %ld/%ld \n", t_stats[t_id].received_r_reps_mes_num, t_stats[t_id].received_r_reps);

  for (uint8_t i = 0; i < MACHINE_NUM; i++) {
    if (i == machine_id) continue;
    cyan_printf("FROM/ TO MACHINE %u \n", i);
    yellow_printf("Acks Received %lu/%lu from machine id %u \n", t_stats[t_id].per_worker_acks_mes_received[i],
                  t_stats[t_id].per_worker_acks_received[i], i);
    yellow_printf("Writes Received %lu from machine id %u\n", t_stats[t_id].per_worker_writes_received[i], i);
    yellow_printf("Acks Sent %lu/%lu to machine id %u \n", t_stats[t_id].per_worker_acks_mes_sent[i],
                 t_stats[t_id].per_worker_acks_sent[i], i);

  }
//  yellow_printf("Reads sent %ld/%ld \n", t_stats[t_id].r_reps_sent_mes_num, t_stats[t_id].r_reps_sent );
}


// Leader checks its debug counters
static inline void check_debug_cntrs(uint32_t *credit_debug_cnt, uint32_t *wait_dbg_counter,
                                     struct pending_ops *p_ops, void *buf,
                                     uint32_t r_pull_ptr, uint32_t w_pull_ptr,
                                     uint32_t ack_pull_ptr, uint32_t r_rep_pull_ptr,
                                     uint16_t t_id)
{

//  volatile struct  w_message_ud_req *w_buffer =
//    (volatile struct w_message_ud_req *)(buf + ACK_BUF_SIZE);
//  volatile struct  r_message_ud_req *r_buffer =
//    (volatile struct r_message_ud_req *)(cb->dgram_buf + ACK_BUF_SIZE + W_BUF_SIZE);

  // ACKS
  if (unlikely(wait_dbg_counter[ACK_QP_ID] > M_512)) {
    red_printf("Worker %d waits for acks \n", t_id);
    if (VERBOSE_DBG_COUNTER) {
      struct ack_message_ud_req *ack_buf = (struct ack_message_ud_req *) (buf);
      struct ack_message *ack = &ack_buf[ack_pull_ptr].ack;
      uint64_t l_id = *(uint64_t *) ack->local_id;
      uint8_t message_opc = ack->opcode;
      cyan_printf("Wrkr %d, polling on index %u, polled opc %u, 1st ack opcode: %u, l_id %lu, expected l_id %lu\n",
                  t_id, ack_pull_ptr, message_opc, ack->opcode, l_id, p_ops->local_w_id);
      MOD_ADD(ack_pull_ptr, ACK_BUF_SLOTS);
      ack = &ack_buf[ack_pull_ptr].ack;
      l_id = *(uint64_t *) ack->local_id;
      message_opc = ack->opcode;
      cyan_printf("Next index %u,polled opc %u, 1st ack opcode: %u, l_id %lu, expected l_id %lu\n",
                  ack_pull_ptr, message_opc, ack->opcode, l_id, p_ops->local_w_id);
      for (int i = 0; i < ACK_BUF_SLOTS; ++i) {
        if (ack_buf[i].ack.opcode == CACHE_OP_ACK) {
          green_printf("GOOD OPCODE in index %d, l_id %u \n", i, *(uint64_t *) ack_buf[i].ack.local_id);
        } else
          red_printf("BAD OPCODE in index %d, l_id %u, from machine: %u  \n", i, *(uint64_t *) ack_buf[i].ack.local_id,
                     ack_buf[i].ack.m_id);

      }
    }
    print_wrkr_stats(t_id);
    wait_dbg_counter[ACK_QP_ID] = 0;
    //exit(0);
  }
  // R_REPS
  if (unlikely(wait_dbg_counter[R_REP_QP_ID] > M_512)) {
    red_printf("Worker %d waits for r_reps \n", t_id);
    if (VERBOSE_DBG_COUNTER) {
      struct r_rep_message_ud_req *r_rep_buf =
        (struct r_rep_message_ud_req *) (buf + ACK_BUF_SIZE + W_BUF_SIZE + R_BUF_SIZE);
      struct r_rep_message *r_rep = &r_rep_buf[r_rep_pull_ptr].r_rep_mes;
      uint64_t l_id = *(uint64_t *) r_rep->l_id;
      uint8_t message_opc = r_rep->opcode;
      cyan_printf("Wrkr %d, polling on index %u, polled opc %u, 1st r_rep opcode: %u, l_id %lu, expected l_id %lu\n",
                  t_id, r_rep_pull_ptr, message_opc, r_rep->opcode, l_id, p_ops->local_r_id);
      MOD_ADD(r_rep_pull_ptr, R_REP_BUF_SLOTS);
      r_rep = &r_rep_buf[r_rep_pull_ptr].r_rep_mes;
      l_id = *(uint64_t *) r_rep->l_id;
      message_opc = r_rep->opcode;
      cyan_printf("Next index %u,polled opc %u, 1st r_rep opcode: %u, l_id %lu, expected l_id %lu\n",
                  r_rep_pull_ptr, message_opc, r_rep->opcode, l_id, p_ops->local_r_id);
      for (int i = 0; i < R_REP_BUF_SLOTS; ++i) {
        if (r_rep_buf[i].r_rep_mes.opcode == READ_REPLY) {
          green_printf("GOOD OPCODE in index %d, l_id %u \n", i, *(uint64_t *) r_rep_buf[i].r_rep_mes.l_id);
        } else
          red_printf("BAD OPCODE in index %d, l_id %u, from machine: %u  \n", i,
                     *(uint64_t *) r_rep_buf[i].r_rep_mes.l_id,
                     r_rep_buf[i].r_rep_mes.m_id);

      }
    }
    print_wrkr_stats(t_id);
    wait_dbg_counter[R_REP_QP_ID] = 0;
    //exit(0);
  }
  if (unlikely(wait_dbg_counter[R_QP_ID] > M_512)) {
    red_printf("Worker %d waits for reads \n", t_id);
    print_wrkr_stats(t_id);
    wait_dbg_counter[R_QP_ID] = 0;
  }
  if (unlikely(credit_debug_cnt[W_VC] > M_512)) {
    red_printf("Worker %d lacks write credits \n", t_id);
    print_wrkr_stats(t_id);
    credit_debug_cnt[W_VC] = 0;
  }
  if (unlikely(credit_debug_cnt[R_VC] > M_512)) {
    red_printf("Worker %d lacks read credits \n", t_id);
    print_wrkr_stats(t_id);
    credit_debug_cnt[R_VC] = 0;
  }
}


/* ---------------------------------------------------------------------------
//------------------------------ TRACE---------------------------------------
//---------------------------------------------------------------------------*/
// Worker inserts a new local read to the read fifo it maintains
static inline void insert_read(struct pending_ops *p_ops, struct cache_op *read, uint16_t t_id)
{
  struct r_message *r_mes = p_ops->r_fifo->r_message;
  uint32_t r_mes_ptr = p_ops->r_fifo->push_ptr;
  uint8_t inside_r_ptr = r_mes[r_mes_ptr].coalesce_num;
  uint32_t r_ptr = p_ops->r_push_ptr;
  //  printf("Insert a r_rep %u \n", *(uint32_t *)r_rep);
  memcpy(&r_mes[r_mes_ptr].read[inside_r_ptr].ts, (void *)&read->key.meta.m_id, TS_TUPLE_SIZE + TRUE_KEY_SIZE);
  memcpy(&p_ops->read_info[r_ptr].ts_to_read, (void *)&read->key.meta.m_id, TS_TUPLE_SIZE + TRUE_KEY_SIZE);
  r_mes[r_mes_ptr].read[inside_r_ptr].opcode = read->opcode;
  //if (read->opcode == CACHE_OP_LIN_PUT) r_mes[r_mes_ptr].read[inside_r_ptr].opcode = CACHE_OP_LIN_PUT;
  //else r_mes[r_mes_ptr].read[inside_r_ptr].opcode = read->opcode;
  if (inside_r_ptr == 0) {
    p_ops->r_fifo->backward_ptrs[r_mes_ptr] = r_ptr;
    uint64_t message_l_id = (uint64_t) (p_ops->local_r_id + p_ops->r_size);
    if (ENABLE_ASSERTIONS) {
      if (message_l_id > MAX_R_COALESCE) {
        uint32_t prev_r_mes_ptr = (r_mes_ptr + R_FIFO_SIZE - 1) % R_FIFO_SIZE;
        uint64_t prev_l_id = *(uint64_t *) r_mes[prev_r_mes_ptr].l_id;
        uint8_t prev_coalesce = r_mes[prev_r_mes_ptr].coalesce_num;
        if (message_l_id != prev_l_id + prev_coalesce) {
          red_printf("Wrkr: %u Read: Current message l_id %u, previous message l_id %u , previous coalesce %u\n",
                     t_id, message_l_id, prev_l_id, prev_coalesce);
        }
      }
    }
    // printf("message_lid %lu, local_rid %lu, p_ops r_size %u \n", message_l_id, p_ops->local_r_id, p_ops->r_size);
    memcpy(r_mes[r_mes_ptr].l_id, &message_l_id, 8);
  }

  if (ENABLE_ASSERTIONS) {
    if (p_ops->r_state[r_ptr] != INVALID)
      red_printf("Worker %u r_state %d at r_ptr %u, cache hits %lu, r_size %u \n",
                 t_id, p_ops->r_state[r_ptr], r_ptr,
                 t_stats[t_id].cache_hits_per_thread, p_ops->r_size);
    //					printf("Sent %d, Valid %d, Ready %d \n", SENT, VALID, READY);
    assert(p_ops->r_state[r_ptr] == INVALID);
    assert(keys_are_equal((struct cache_key *) (((void *)&r_mes[r_mes_ptr].read[inside_r_ptr]) - 3),
                          (struct cache_key *)read));
  }
  p_ops->r_state[r_ptr] = VALID;
  if (read->opcode == OP_ACQUIRE)
    memcpy(&p_ops->r_session_id[r_ptr], read, SESSION_BYTES); // session id has to fit in 3 bytes
  if (ENABLE_ASSERTIONS) assert(p_ops->r_session_id[r_ptr] <= SESSIONS_PER_THREAD);
  MOD_ADD(p_ops->r_push_ptr, PENDING_READS);
  p_ops->r_size++;
  p_ops->r_fifo->bcast_size++;
  r_mes[r_mes_ptr].coalesce_num++;
  if (r_mes[r_mes_ptr].coalesce_num == MAX_R_COALESCE) {
    MOD_ADD(p_ops->r_fifo->push_ptr, R_FIFO_SIZE);
    r_mes[p_ops->r_fifo->push_ptr].coalesce_num = 0;
  }
}


// Insert a new local or remote write to the pending writes
static inline void insert_write(struct pending_ops *p_ops, struct cache_op *write, uint8_t source,
                                uint32_t r_pull_ptr, uint16_t t_id)
{
  struct read_info *r_info = NULL;
  if (source == FROM_READ) r_info = &p_ops->read_info[r_pull_ptr];
  else if (source == LIN_WRITE) r_info = (struct read_info *) write;

  struct w_message *w_mes = p_ops->w_fifo->w_message;
  uint32_t w_mes_ptr = p_ops->w_fifo->push_ptr;
  uint8_t inside_w_ptr = w_mes[w_mes_ptr].w_num;
  uint32_t w_ptr = p_ops->w_push_ptr;
  //printf("Insert a write %u \n", *(uint32_t *)write);
  if (source == FROM_TRACE) {
    // if the write is a release put it on a new message to
    // guarantee it is not batched with writes from the same session
    if (write->opcode == OP_RELEASE && inside_w_ptr > 0) {
      MOD_ADD(p_ops->w_fifo->push_ptr, W_FIFO_SIZE);
      w_mes_ptr = p_ops->w_fifo->push_ptr;
      w_mes[w_mes_ptr].w_num = 0;
      inside_w_ptr = 0;
    }
    memcpy(w_mes[w_mes_ptr].write[inside_w_ptr].version, (void *) &write->key.meta.version,
           4 + TRUE_KEY_SIZE + 2 + VALUE_SIZE);
    w_mes[w_mes_ptr].write[inside_w_ptr].m_id = (uint8_t) machine_id;
  }
  else {
    memcpy(&w_mes[w_mes_ptr].write[inside_w_ptr], &r_info->ts_to_read, TS_TUPLE_SIZE + TRUE_KEY_SIZE);
    memcpy(w_mes[w_mes_ptr].write[inside_w_ptr].value, r_info->value, VALUE_SIZE);
    if (r_info->opcode == OP_ACQUIRE) w_mes[w_mes_ptr].write[inside_w_ptr].opcode = OP_ACQUIRE;
    else w_mes[w_mes_ptr].write[inside_w_ptr].opcode = CACHE_OP_PUT;
  }
  if (inside_w_ptr == 0) {
    p_ops->w_fifo->backward_ptrs[w_mes_ptr] = w_ptr;
    uint64_t message_l_id = (uint64_t) (p_ops->local_w_id + p_ops->w_size);
    if (ENABLE_ASSERTIONS) {
      if (ENABLE_ASSERTIONS) assert ((*(uint32_t *)w_mes[w_mes_ptr].write[inside_w_ptr].version) < B_4_EXACT);
      if((*(uint32_t *)w_mes[w_mes_ptr].write[inside_w_ptr].version) % 2 != 0) {
        red_printf("Version to insert %u, comes from read %u \n",
                   *(uint32_t *)w_mes[w_mes_ptr].write[inside_w_ptr].version, source);
        assert (false);
      }

      if (message_l_id > MAX_W_COALESCE) {
        uint32_t prev_w_mes_ptr = (w_mes_ptr + W_FIFO_SIZE - 1) % W_FIFO_SIZE;
        uint64_t prev_l_id = *(uint64_t *) w_mes[prev_w_mes_ptr].l_id;
        uint8_t prev_coalesce = w_mes[prev_w_mes_ptr].w_num;
        if (message_l_id != prev_l_id + prev_coalesce) {
          red_printf("Current message l_id %u, previous message l_id %u , previous coalesce %u\n",
                     message_l_id, prev_l_id, prev_coalesce);
        }
      }
    }
    //printf("message_lid %lu, local_wid %lu, p_ops w_size %u \n", message_l_id, p_ops->local_w_id, p_ops->w_size);
    memcpy(w_mes[w_mes_ptr].l_id, &message_l_id, 8);
  }
  if (ENABLE_ASSERTIONS) {
    if (unlikely(p_ops->w_state[w_ptr] != INVALID))
      red_printf("Worker %u w_state %d at w_ptr %u, cache hits %lu, w_size %u \n",
                 t_id, p_ops->w_state[w_ptr], w_ptr,
                 t_stats[t_id].cache_hits_per_thread, p_ops->w_size);
     //					printf("Sent %d, Valid %d, Ready %d \n", SENT, VALID, READY);
    assert(p_ops->w_state[w_ptr] == INVALID);
    //if (!comes_from_read)
      //assert(keys_are_equal((struct cache_key *) &w_mes[w_mes_ptr].write[inside_w_ptr],
      //                     (struct cache_key *)write));
  }
  p_ops->w_state[w_ptr] = VALID;
  if (source != FROM_READ) {
    if (write->opcode == OP_RELEASE) memcpy(&p_ops->w_session_id[w_ptr], write, SESSION_BYTES);
    if (source == LIN_WRITE) memset(write, 0, 3); // empty the read info such that it can be reused
  }
  else if (r_info->opcode == OP_ACQUIRE) p_ops->w_session_id[w_ptr] = p_ops->r_session_id[r_pull_ptr];
  MOD_ADD(p_ops->w_push_ptr, PENDING_WRITES);
  if (ENABLE_ASSERTIONS) assert(p_ops->w_push_ptr != p_ops->w_pull_ptr);
  p_ops->w_size++;
  p_ops->w_fifo->bcast_size++;
  w_mes[w_mes_ptr].w_num++;
  if (w_mes[w_mes_ptr].w_num == MAX_W_COALESCE) {
    MOD_ADD(p_ops->w_fifo->push_ptr, W_FIFO_SIZE);
    w_mes[p_ops->w_fifo->push_ptr].w_num = 0;
  }
}

// setup a new r_rep entry
static inline void set_up_r_rep_entry( struct r_rep_fifo *r_rep_fifo, uint8_t rem_m_id, uint64_t l_id,
                                       struct r_rep_message *r_rep_mes)
{
  r_rep_fifo->rem_m_id[r_rep_fifo->push_ptr] = rem_m_id;
  //r_rep_mes[r_rep_fifo->push_ptr].credits = 1;
  memcpy(r_rep_mes[r_rep_fifo->push_ptr].l_id, &l_id, 8);
  r_rep_fifo->message_sizes[r_rep_fifo->push_ptr] = R_REP_MES_HEADER;
}

// Insert a new r_rep to the r_rep reply fifo
static inline void insert_r_rep(struct pending_ops *p_ops, struct ts_tuple *local_ts,
                                struct ts_tuple *remote_ts, uint64_t l_id, uint16_t t_id,
                                uint8_t rem_m_id, uint16_t op_i, uint8_t* payload, bool is_lin_put) {
  struct r_rep_fifo *r_rep_fifo = p_ops->r_rep_fifo;
  struct r_rep_message *r_rep_mes = r_rep_fifo->r_rep_message;

   /* A reply message corresponds to exactly one read message
    * to avoid reasoning about l_ids, credits and so on */

  // If the r_rep has a different recipient or different l_id then create a new message
  // Because the criterion to advance the push ptr is on creating a new message,
  // the pull ptr has to start from 1
  if ((rem_m_id != r_rep_fifo->rem_m_id[r_rep_fifo->push_ptr]) ||
       l_id != *(uint64_t *)r_rep_mes[r_rep_fifo->push_ptr].l_id) {
    MOD_ADD(r_rep_fifo->push_ptr, R_REP_FIFO_SIZE);
    r_rep_mes[r_rep_fifo->push_ptr].coalesce_num = 0;
    r_rep_fifo->mes_size++;
    set_up_r_rep_entry(r_rep_fifo, rem_m_id, l_id, r_rep_mes);
  }
  uint32_t r_rep_mes_ptr = r_rep_fifo->push_ptr;
  uint32_t inside_r_rep_ptr = r_rep_fifo->message_sizes[r_rep_fifo->push_ptr]; // This pointer is in bytes
  r_rep_fifo->message_sizes[r_rep_fifo->push_ptr] += R_REP_SMALL_SIZE;
  struct r_rep_big *r_rep = (struct r_rep_big *) (((void *)&r_rep_mes[r_rep_mes_ptr]) + inside_r_rep_ptr);

  enum ts_compare ts_comp = compare_ts(local_ts, remote_ts);
  if (machine_id == 0 && R_TO_W_DEBUG) {
    if (ts_comp == EQUAL) green_printf("L/R:  m_id: %u/%u version %u/%u \n", local_ts->m_id, remote_ts->m_id,
              *(uint32_t *) local_ts->version, *(uint32_t *) remote_ts->version);
    else red_printf("L/R:  m_id: %u/%u version %u/%u \n", local_ts->m_id, remote_ts->m_id,
                    *(uint32_t *) local_ts->version, *(uint32_t *) remote_ts->version);
  }
  switch (ts_comp) {
    case SMALLER: // local is smaller than remote
      //if (DEBUG_TS) printf("Read TS is smaller \n");
      r_rep->opcode = TS_SMALLER;
      break;
    case EQUAL:
      //if (DEBUG_TS) /printf("Read TS are equal \n");
      r_rep->opcode = TS_EQUAL;
      break;
    case GREATER: // local is greater than remote
      memcpy(&r_rep->ts, local_ts, TS_TUPLE_SIZE);
      if (ENABLE_LIN && is_lin_put) {
        r_rep->opcode = TS_GREATER_LIN_PUT;
        r_rep_fifo->message_sizes[r_rep_fifo->push_ptr] += (R_REP_LIN_PUT_SIZE - R_REP_SMALL_SIZE);
      }
      else {
        if (DEBUG_TS) printf("Read TS is greater \n");
        r_rep->opcode = TS_GREATER;
        memcpy(r_rep->value, payload, VALUE_SIZE);
        r_rep_fifo->message_sizes[r_rep_fifo->push_ptr] += (R_REP_SIZE - R_REP_SMALL_SIZE);
      }
      break;
    default:
      assert(false);
  }
  p_ops->r_rep_fifo->total_size++;
  r_rep_mes[r_rep_mes_ptr].coalesce_num++;
  if (ENABLE_ASSERTIONS) {
    if (is_lin_put) assert(ENABLE_LIN);
    assert(r_rep_fifo->message_sizes[r_rep_fifo->push_ptr] <= R_REP_SEND_SIZE);
    assert(r_rep_mes[r_rep_mes_ptr].coalesce_num <= MAX_R_REP_COALESCE);
  }
}

// Use this to r_rep the trace, propagate reqs to the cache and maintain their r_rep/write fifos
static inline uint32_t batch_from_trace_to_cache(uint32_t trace_iter, uint16_t t_id,
                                                 struct trace_command_uni *trace, struct cache_op *ops,
                                                 struct pending_ops *p_ops, struct mica_resp *resp)
{
  int i = 0, op_i = 0;
  uint16_t writes_num = 0, reads_num = 0;
  uint8_t is_update = 0;
  int working_session = -1;
  if (p_ops->all_sessions_stalled) return trace_iter;
  for (i = 0; i < SESSIONS_PER_THREAD; i++) {
    if (!p_ops->session_has_pending_op[i]) {
      working_session = i;
      break;
    }
  }
  //   printf("working session = %d\n", working_session);
  if (ENABLE_ASSERTIONS) assert(working_session != -1);

  //green_printf("op_i %d , trace_iter %d, trace[trace_iter].opcode %d \n", op_i, trace_iter, trace[trace_iter].opcode);
  while (op_i < MAX_OP_BATCH && working_session < SESSIONS_PER_THREAD) {
    if (ENABLE_ASSERTIONS) assert(trace[trace_iter].opcode != NOP);
    is_update = (uint8_t) IS_WRITE(trace[trace_iter].opcode);
    // Create some back pressure from the buffers, since the sessions may never be stalled
    if (is_update) writes_num++; else reads_num++;
    if (p_ops->w_size + writes_num >= PENDING_WRITES || p_ops->r_size + reads_num >= PENDING_READS)
      break;
    memcpy(((void *)&(ops[op_i].key)) + TRUE_KEY_SIZE, trace[trace_iter].key_hash, TRUE_KEY_SIZE);
    ops[op_i].opcode = trace[trace_iter].opcode;
    ops[op_i].val_len = is_update ? (uint8_t) (VALUE_SIZE >> SHIFT_BITS) : (uint8_t) 0;
    if (ops[op_i].opcode == OP_RELEASE || ops[op_i].opcode == OP_ACQUIRE) {
      p_ops->session_has_pending_op[working_session] = true;
      memcpy(&ops[op_i], &working_session, SESSION_BYTES);// Overload this field to associate a session with an op
    }
    //yellow_printf("BEFORE: OP_i %u -> session %u, opcode: %u \n", op_i, working_session, ops[op_i].opcode);
    while (p_ops->session_has_pending_op[working_session]) {
      working_session++;
      if (working_session == SESSIONS_PER_THREAD) {
        p_ops->all_sessions_stalled = true;
        break;
      }
    }
    //cyan_printf("thread %d  next working session %d total ops %d\n", t_id, working_session, op_i);
    if (ENABLE_ASSERTIONS) {
      assert(WRITE_RATIO > 0 || is_update == 0);
      if (is_update) assert(ops[op_i].val_len > 0);
    }
    resp[op_i].type = EMPTY;
    trace_iter++;
    if (trace[trace_iter].opcode == NOP) trace_iter = 0;
    op_i++;

  }

  t_stats[t_id].cache_hits_per_thread += op_i;
  cache_batch_op_trace(op_i, t_id, &ops, resp, p_ops);
  // assert(op_i + p_ops->w_size == SESSIONS_PER_THREAD);
  //cyan_printf("thread %d  adds %d ops\n", t_id, op_i);
  for (i = 0; i < op_i; i++) {
    // green_printf("After: OP_i %u -> session %u \n", i, *(uint32_t *) &ops[i]);
    if (resp[i].type == CACHE_MISS)  {
      yellow_printf("Cache_miss, session %d \n", *(uint32_t *) &ops[i]);
      if (ops[op_i].opcode == OP_RELEASE || ops[op_i].opcode == OP_ACQUIRE) {
        p_ops->session_has_pending_op[*(uint32_t *) &ops[i]] = false;
        p_ops->all_sessions_stalled = false;
      }
    }
    else if (resp[i].type == CACHE_LOCAL_GET_SUCCESS) ;
    else if (ops[i].opcode == CACHE_OP_PUT || ops[i].opcode == OP_RELEASE)
      insert_write(p_ops, &ops[i], FROM_TRACE, 0, t_id);
    else {
      insert_read(p_ops, &ops[i], t_id);
    }
  }
  return trace_iter;
}


/* ---------------------------------------------------------------------------
//------------------------------ BROADCASTS -----------------------------
//---------------------------------------------------------------------------*/

// Update the quorum info, use this one a timeout
static inline void update_q_info(struct quorum_info *q_info,
                                 uint16_t credits[][MACHINE_NUM], uint16_t min_credits, uint8_t vc)
{
  uint8_t i, rm_id;
  q_info->missing_num = 0;
  q_info->active_num = 0;
  for (i = 0; i < MACHINE_NUM; i++) {
    if (i == machine_id) continue;
    rm_id = mid_to_rmid(i);
    if (credits[vc][i] < min_credits) {
      q_info->missing_ids[q_info->missing_num] = i;
      q_info->missing_num++;
      q_info->send_vector[rm_id] = false;
    }
    else {
      q_info->active_ids[q_info->active_num] = i;
      q_info->active_num++;
      q_info->send_vector[rm_id] = true;
    }
  }
  q_info->first_active_rm_id = mid_to_rmid(q_info->active_ids[0]);
  q_info->last_active_rm_id = mid_to_rmid(q_info->active_ids[q_info->active_num - 1]);
  if (DEBUG_QUORUM) print_q_info(q_info);

  if (ENABLE_ASSERTIONS) {
    assert(q_info->missing_num <= REM_MACH_NUM);
    assert(q_info->active_num <= REM_MACH_NUM);
  }
}

// Bring back a machine
static inline void revive_machine(struct quorum_info *q_info,
                                 uint8_t revived_mach_id)
{

  uint8_t rm_id = mid_to_rmid(revived_mach_id);
  if (ENABLE_ASSERTIONS) {
    assert(revived_mach_id < MACHINE_NUM);
    assert(revived_mach_id != machine_id);
    assert(q_info->missing_num > 0);
    assert(q_info->send_vector[rm_id] == false);
  }
  // Fix the send vector and update the rest based on that,
  // because using the credits may not be reliable here
  q_info->send_vector[rm_id] = true;
  q_info->missing_num = 0;
  q_info->active_num = 0;
  for (uint8_t i = 0; i < REM_MACH_NUM; i++) {
    uint8_t m_id = rmid_to_mid(i);
    if (!q_info->send_vector[i]) {
      q_info->missing_ids[q_info->missing_num] = m_id;
      q_info->missing_num++;
    }
    else {
      q_info->active_ids[q_info->active_num] = m_id;
      q_info->active_num++;
    }
  }
  q_info->first_active_rm_id = mid_to_rmid(q_info->active_ids[0]);
  q_info->last_active_rm_id = mid_to_rmid(q_info->active_ids[q_info->active_num - 1]);
  if (DEBUG_QUORUM) print_q_info(q_info);
  for (uint16_t i = 0; i < q_info->missing_num; i++)
    if (DEBUG_QUORUM) green_printf("After: Missing position %u, missing id %u, id to revive\n",
                                   i, q_info->missing_ids[i], revived_mach_id);
}

// Update the links between the send Work Requests for broadcasts given the quorum information
static inline void update_bcast_wr_links (struct quorum_info *q_info, struct ibv_send_wr *wr, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) assert(MESSAGES_IN_BCAST == REM_MACH_NUM);
  uint8_t prev_i = 0, avail_mach = 0;
  green_printf("Worker %u fixing the links between the wrs \n", t_id);
  for (uint8_t i = 0; i < REM_MACH_NUM; i++) {
    wr[i].next = NULL;
    if (q_info->send_vector) {
      if (avail_mach > 0) {
        for (uint16_t j = 0; j < MAX_BCAST_BATCH; j++) {
          yellow_printf("Worker %u, wr %d points to %d\n", t_id, (REM_MACH_NUM * j) + prev_i, (REM_MACH_NUM * j) + i);
          wr[(REM_MACH_NUM * j) + prev_i].next = &wr[(REM_MACH_NUM * j) + i];
        }
      }
      avail_mach++;
      prev_i = i;
    }
  }
}

// Update the quorum info, use this one a timeout
static inline void decrease_credits(uint16_t credits[][MACHINE_NUM], struct quorum_info *q_info,
                                    uint16_t mes_sent, uint8_t vc)
{
  for (uint8_t i = 0; i < q_info->active_num; i++) {
    if (ENABLE_ASSERTIONS) assert(credits[vc][q_info->active_ids[i]] >= mes_sent);
    credits[vc][q_info->active_ids[i]] -= mes_sent;
  }
}

//Checks if there are enough credits to perform a broadcast -- protocol independent
static inline bool check_bcast_all_credits_depr(uint16_t credits[][MACHINE_NUM], struct hrd_ctrl_blk *cb, struct ibv_wc *credit_wc,
                                                uint32_t *credit_debug_cnt, uint8_t vc)
{
  bool poll_for_credits = false;
  uint16_t j;
  for (j = 0; j < MACHINE_NUM; j++) {
    if (j == machine_id) continue;
    if (credits[vc][j] == 0) {
      poll_for_credits = true;
      break;
    }
  }
  //if (ENABLE_ASSERTIONS) {
  if (poll_for_credits) credit_debug_cnt[vc]++;
  else credit_debug_cnt[vc] = 0;
  //}
  // There are no explicit credit messages for the reads or write messages
  return !poll_for_credits;

}

// Check credits, first see if there are credits from all active nodes, then if not and enough time has passed,
// transition to write_quorum broadcasts
static inline bool check_bcast_credits(uint16_t credits[][MACHINE_NUM], struct quorum_info *q_info,
                                       uint32_t *time_out_cnt, uint8_t vc,
                                       uint16_t *available_credits, struct ibv_send_wr *send_wr,
                                       uint16_t min_credits,
                                       uint32_t *credit_debug_cnt, uint16_t t_id)
{
  uint16_t i;
  // First check the active ids, to have a fast path when there are not enough credits
  for (i = 0; i < q_info->active_num; i++) {
    if (credits[vc][q_info->active_ids[i]] < min_credits) {
      time_out_cnt[vc]++;
      if (time_out_cnt[vc] == CREDIT_TIMEOUT) {
        if (DEBUG_QUORUM) red_printf("Worker %u timed_out on machine %u \n", t_id, q_info->active_ids[i]);
        update_q_info(q_info, credits, min_credits, vc);
        update_bcast_wr_links(q_info, send_wr, t_id);
        time_out_cnt[vc] = 0;
      }
      return false;
    }
  }
  time_out_cnt[vc] = 0;

  // then check the missing credits to see if we need to change the configuration
  if (q_info->missing_num > 0) {
    for (i = 0; i < q_info->missing_num; i++) {
      if (credits[W_VC][q_info->missing_ids[i]] == W_CREDITS &&
          credits[R_VC][q_info->missing_ids[i]] == R_CREDITS ) {
        if (DEBUG_QUORUM) red_printf("Worker %u revives machine %u \n", t_id, q_info->missing_ids[i]);
        revive_machine(q_info, q_info->missing_ids[i]);
        update_bcast_wr_links(q_info, send_wr, t_id);
      }
    }
  }

  //finally count credits
  uint16_t avail_cred = K_64_;
  //printf("avail cred %u\n", avail_cred);
  for (i = 0; i < q_info->active_num; i++) {
    if (credits[vc][q_info->active_ids[i]] < avail_cred)
      avail_cred = credits[vc][q_info->active_ids[i]];
  }
  *available_credits = avail_cred;
  return true;
}



// Broadcast logic uses this function to post appropriate number of credit recvs before sending broadcasts
static inline void post_recvs_and_batch_bcasts_to_NIC(uint16_t br_i, struct hrd_ctrl_blk *cb,
                                                      struct ibv_send_wr *send_wr,
                                                      struct ibv_recv_wr *credit_recv_wr,
                                                      uint16_t *credit_recv_counter, uint8_t qp_id)
{
  uint16_t j;
  int ret;
  struct ibv_send_wr *bad_send_wr;
  uint32_t max_credit_recvs = 0;
  if (max_credit_recvs > 0) {
    struct ibv_recv_wr *bad_recv_wr;
    if (*credit_recv_counter > 0) { // Must post receives for credits
      if (ENABLE_ASSERTIONS == 1) assert ((*credit_recv_counter) * REM_MACH_NUM <= max_credit_recvs);
      for (j = 0; j < REM_MACH_NUM * (*credit_recv_counter); j++) {
        credit_recv_wr[j].next = (j == (REM_MACH_NUM * (*credit_recv_counter)) - 1) ?
                                 NULL : &credit_recv_wr[j + 1];
      }
//    cyan_printf("Leader posting a credit receive\n");
      ret = ibv_post_recv(cb->dgram_qp[FC_QP_ID], &credit_recv_wr[0], &bad_recv_wr);
      if (ENABLE_ASSERTIONS) CPE(ret, "ibv_post_recv error: posting recvs for credits before broadcasting", ret);
      *credit_recv_counter = 0;
    }
  }
  // Batch the broadcasts to the NIC
  if (br_i > 0) {
    send_wr[(br_i * MESSAGES_IN_BCAST) - 1].next = NULL;
    ret = ibv_post_send(cb->dgram_qp[qp_id], &send_wr[0], &bad_send_wr);
    if (ENABLE_ASSERTIONS) CPE(ret, "Broadcast ibv_post_send error", ret);
  }
}

// Post a quorum broadcast and apost the appropriate receives for it
static inline void post_quorum_broadasts_and_recvs(struct recv_info *recv_info, uint32_t recvs_to_post_num,
                                                   struct quorum_info *q_info, uint16_t br_i, uint64_t br_tx,
                                                   struct ibv_send_wr *send_wr, struct ibv_qp *send_qp,
                                                   int enable_inlining)
{
  struct ibv_send_wr *bad_send_wr;
  if (recvs_to_post_num > 0) {
    // printf("Wrkr %d posting %d recvs\n", t_id,  recvs_to_post_num);
    if (recvs_to_post_num) post_recvs_with_recv_info(recv_info, recvs_to_post_num);
    recv_info->posted_recvs += recvs_to_post_num;
  }
  if (DEBUG_SS_BATCH)
    green_printf("Sending %u bcasts, total %lu \n", br_i, br_tx);

  send_wr[((br_i - 1) * MESSAGES_IN_BCAST) + q_info->last_active_rm_id].next = NULL;
  int ret = ibv_post_send(send_qp, &send_wr[q_info->first_active_rm_id], &bad_send_wr);
  if (ENABLE_ASSERTIONS) CPE(ret, "Broadcast ibv_post_send error", ret);
  if (!ENABLE_ADAPTIVE_INLINING)
    send_wr[q_info->first_active_rm_id].send_flags = enable_inlining == 1 ? IBV_SEND_INLINE : 0;
}

// Form the Broadcast work request for the write
static inline void forge_w_wr(uint32_t w_mes_i, struct pending_ops *p_ops,
                              struct quorum_info *q_info,
                              struct hrd_ctrl_blk *cb, struct ibv_sge *send_sgl,
                              struct ibv_send_wr *send_wr, uint64_t *w_br_tx,
                              uint16_t br_i, uint16_t credits[][MACHINE_NUM],
                              uint8_t vc, uint16_t t_id) {
  uint16_t i;
  struct ibv_wc signal_send_wc;
  struct w_message *w_mes = &p_ops->w_fifo->w_message[w_mes_i];
  uint32_t backward_ptr = p_ops->w_fifo->backward_ptrs[w_mes_i];
  uint16_t coalesce_num = w_mes->w_num;
  send_sgl[br_i].length = W_MES_HEADER + coalesce_num * sizeof(struct write);
  send_sgl[br_i].addr = (uint64_t) (uintptr_t) w_mes;
  if (ENABLE_ADAPTIVE_INLINING)
    adaptive_inlining(send_sgl[br_i].length, &send_wr[br_i * MESSAGES_IN_BCAST], MESSAGES_IN_BCAST);
  for (i = 0; i < coalesce_num; i++) {
    if (w_mes->write[i].opcode == CACHE_OP_PUT)
      p_ops->w_state[(backward_ptr + i) % PENDING_WRITES] = SENT_PUT;
    else p_ops->w_state[(backward_ptr + i) % PENDING_WRITES] = SENT_RELEASE;
    if (DEBUG_WRITES)
      printf("Write %d, val-len %u, message w_size %d\n", i, w_mes->write[i].val_len,
             send_sgl[br_i].length);
    if (ENABLE_ASSERTIONS) {
      assert(w_mes->write[i].val_len == VALUE_SIZE >> SHIFT_BITS);
      assert(w_mes->write[i].opcode == CACHE_OP_PUT ||
             w_mes->write[i].opcode == OP_RELEASE ||
             w_mes->write[i].opcode == OP_ACQUIRE);
      //assert(w_mes->write[i].m_id == machine_id); // not true because reads get converted to writes with random m_ids
    }
  }
  if (DEBUG_WRITES)
    green_printf("Wrkr %d : I BROADCAST a write message %d of %u writes with mes_size %u, with credits: %d, lid: %u  \n",
                 t_id, w_mes->write[coalesce_num - 1].opcode, coalesce_num, send_sgl[br_i].length,
                 credits[vc][(machine_id + 1) % MACHINE_NUM], *(uint64_t*)w_mes->l_id);

  // Do a Signaled Send every W_BCAST_SS_BATCH broadcasts (W_BCAST_SS_BATCH * (MACHINE_NUM - 1) messages)
  if ((*w_br_tx) % W_BCAST_SS_BATCH == 0) {
    if (DEBUG_SS_BATCH)
      printf("Wrkr %u Sending signaled the first message, total %lu, br_i %u \n", t_id, *w_br_tx, br_i);
    send_wr[q_info->first_active_rm_id].send_flags |= IBV_SEND_SIGNALED;
  }
  (*w_br_tx)++;
  if ((*w_br_tx) % W_BCAST_SS_BATCH == W_BCAST_SS_BATCH - 1) {
    if (DEBUG_SS_BATCH)
      printf("Wrkr %u POLLING for a send completion in writes, total %lu \n", t_id, *w_br_tx);
    poll_cq(cb->dgram_send_cq[W_QP_ID], 1, &signal_send_wc, POLL_CQ_W);
  }
  // Have the last message of each broadcast pointing to the first message of the next bcast
  if (br_i > 0) {
    send_wr[((br_i - 1) * MESSAGES_IN_BCAST) + q_info->last_active_rm_id].next =
      &send_wr[(br_i * MESSAGES_IN_BCAST) + q_info->first_active_rm_id];
  }

}


// Broadcast Writes
static inline void broadcast_writes(struct pending_ops *p_ops, struct quorum_info *q_info,
                                    uint16_t credits[][MACHINE_NUM], struct hrd_ctrl_blk *cb,
                                    uint32_t *credit_debug_cnt, uint32_t *time_out_cnt,
                                    struct ibv_sge *w_send_sgl, struct ibv_send_wr *w_send_wr,
                                    uint64_t *w_br_tx, struct recv_info *ack_recv_info,
                                    uint16_t t_id, uint32_t *outstanding_writes)
{
  //  printf("Worker %d bcasting writes \n", t_id);
  uint8_t vc = W_VC;
  uint16_t br_i = 0, mes_sent = 0, available_credits = 0;
  uint32_t bcast_pull_ptr = p_ops->w_fifo->bcast_pull_ptr;
  bool is_release;
  if (p_ops->w_fifo->bcast_size > 0) {
    is_release = p_ops->w_fifo->w_message[bcast_pull_ptr].write[0].opcode == OP_RELEASE;
    uint16_t min_credits = is_release ? (uint16_t) W_CREDITS : (uint16_t)1;
    if (!check_bcast_credits(credits, q_info, time_out_cnt, vc,
                             &available_credits, w_send_wr, min_credits, credit_debug_cnt, t_id))
      return;
    if (ENABLE_ASSERTIONS) {
      if (is_release) assert(available_credits == W_CREDITS);
    }
  }
  else return;


  while (p_ops->w_fifo->bcast_size > 0 && mes_sent < available_credits) {
    is_release = p_ops->w_fifo->w_message[bcast_pull_ptr].write[0].opcode == OP_RELEASE;
    if (is_release) {
      if (mes_sent > 0 || available_credits < W_CREDITS) {
        break;
      }
    }
    if (DEBUG_WRITES)
      printf("Wrkr %d has %u write bcasts to send credits %d\n",t_id, p_ops->w_fifo->bcast_size, credits[W_VC][0]);
    // Create the broadcast messages
    forge_w_wr(bcast_pull_ptr, p_ops, q_info, cb,  w_send_sgl, w_send_wr, w_br_tx, br_i, credits, vc, t_id);
    br_i++;
    uint8_t coalesce_num = p_ops->w_fifo->w_message[bcast_pull_ptr].w_num;
    if (ENABLE_ASSERTIONS) {
      assert(p_ops->w_fifo->bcast_size >= coalesce_num);
      (*outstanding_writes) += coalesce_num;
    }
    if (ENABLE_STAT_COUNTING) {
      t_stats[t_id].writes_sent += coalesce_num;
      t_stats[t_id].writes_sent_mes_num++;
    }
    p_ops->w_fifo->bcast_size -= coalesce_num;
    // This message has been sent, do not add other writes to it!
    // this is tricky because releases leave half-filled messages, make sure this is the last message to bcast
    if (coalesce_num < MAX_W_COALESCE && p_ops->w_fifo->bcast_size == 0) {
      //yellow_printf("Broadcasting write with coalesce num %u \n", coalesce_num);
      MOD_ADD(p_ops->w_fifo->push_ptr, W_FIFO_SIZE);
      p_ops->w_fifo->w_message[p_ops->w_fifo->push_ptr].w_num = 0;
    }
    mes_sent++;
    MOD_ADD(bcast_pull_ptr, W_FIFO_SIZE);
    if (br_i == MAX_BCAST_BATCH) {
      post_quorum_broadasts_and_recvs(ack_recv_info, MAX_RECV_ACK_WRS - ack_recv_info->posted_recvs,
                                      q_info, br_i, *w_br_tx, w_send_wr, cb->dgram_qp[W_QP_ID],
                                      W_ENABLE_INLINING);
      br_i = 0;
    }
  }
  if (br_i > 0)
    post_quorum_broadasts_and_recvs(ack_recv_info, MAX_RECV_ACK_WRS - ack_recv_info->posted_recvs,
                                    q_info, br_i, *w_br_tx, w_send_wr, cb->dgram_qp[W_QP_ID],
                                    W_ENABLE_INLINING);

  p_ops->w_fifo->bcast_pull_ptr = bcast_pull_ptr;
  if (mes_sent > 0) decrease_credits(credits, q_info, mes_sent, vc);
}

// Form the Broadcast work request for the red
static inline void forge_r_wr(uint32_t r_mes_i, struct pending_ops *p_ops,
                              struct quorum_info *q_info,
                              struct hrd_ctrl_blk *cb, struct ibv_sge *send_sgl,
                              struct ibv_send_wr *send_wr, uint64_t *r_br_tx,
                              uint16_t br_i, uint16_t credits[][MACHINE_NUM],
                              uint8_t vc, uint16_t t_id) {
  uint16_t i;
  struct ibv_wc signal_send_wc;
  struct r_message *r_mes = &p_ops->r_fifo->r_message[r_mes_i];
  uint32_t backward_ptr = p_ops->r_fifo->backward_ptrs[r_mes_i];
  uint16_t coalesce_num = r_mes->coalesce_num;
  send_sgl[br_i].length = R_MES_HEADER + coalesce_num * sizeof(struct read);
  send_sgl[br_i].addr = (uint64_t) (uintptr_t) r_mes;
  if (ENABLE_ADAPTIVE_INLINING)
    adaptive_inlining(send_sgl[br_i].length, &send_wr[br_i * MESSAGES_IN_BCAST], MESSAGES_IN_BCAST);
  for (i = 0; i < coalesce_num; i++) {
    p_ops->r_state[(backward_ptr + i) % PENDING_READS] = SENT;
    if (DEBUG_READS)
      yellow_printf("Read %d, message mes_size %d, version %u \n", i,
             send_sgl[br_i].length, *(uint32_t *)r_mes->read[i].ts.version);
    if (ENABLE_ASSERTIONS) {
      assert(r_mes->read[i].opcode == CACHE_OP_GET || r_mes->read[i].opcode == CACHE_OP_LIN_PUT ||
             r_mes->read[i].opcode == OP_ACQUIRE || r_mes->read[i].opcode == OP_LIN_RELEASE);
    }
  }
  if (DEBUG_READS)
    green_printf("Wrkr %d : I BROADCAST a read message %d of %u reads with mes_size %u, with credits: %d, lid: %u  \n",
                 t_id, r_mes->read[coalesce_num - 1].opcode, coalesce_num, send_sgl[br_i].length,
                 credits[vc][(machine_id + 1) % MACHINE_NUM], *(uint64_t*)r_mes->l_id);
  //send_wr[0].send_flags = R_ENABLE_INLINING == 1 ? IBV_SEND_INLINE : 0;
  // Do a Signaled Send every R_BCAST_SS_BATCH broadcasts (R_BCAST_SS_BATCH * (MACHINE_NUM - 1) messages)
  if ((*r_br_tx) % R_BCAST_SS_BATCH == 0)
    send_wr[q_info->first_active_rm_id].send_flags |= IBV_SEND_SIGNALED;
  (*r_br_tx)++;
  if ((*r_br_tx) % R_BCAST_SS_BATCH == R_BCAST_SS_BATCH - 1) {
    //printf("Wrkr %u POLLING for a send completion in reads \n", m_id);
    poll_cq(cb->dgram_send_cq[R_QP_ID], 1, &signal_send_wc, POLL_CQ_R);
  }
  // Have the last message of each broadcast pointing to the first message of the next bcast
  if (br_i > 0)
    send_wr[((br_i - 1) * MESSAGES_IN_BCAST) + q_info->last_active_rm_id].next =
      &send_wr[(br_i * MESSAGES_IN_BCAST) + q_info->first_active_rm_id];

}

// Broadcast Reads
static inline void broadcast_reads(struct pending_ops *p_ops,
                                   uint16_t credits[][MACHINE_NUM], struct hrd_ctrl_blk *cb,
                                   struct quorum_info *q_info, uint32_t *credit_debug_cnt,
                                   uint32_t *time_out_cnt,
                                   struct ibv_sge *r_send_sgl, struct ibv_send_wr *r_send_wr,
                                   uint64_t *r_br_tx, struct recv_info *r_rep_recv_info,
                                   uint16_t t_id, uint32_t *outstanding_reads)
{
  //  printf("Worker %d bcasting reads \n", t_id);
  uint8_t vc = R_VC;
  uint16_t reads_sent = 0, br_i = 0, mes_sent = 0, available_credits = 0;
  uint32_t bcast_pull_ptr = p_ops->r_fifo->bcast_pull_ptr;

  if (p_ops->r_fifo->bcast_size > 0) {
    if (!check_bcast_credits(credits, q_info, time_out_cnt, vc,
                             &available_credits, r_send_wr, 1, credit_debug_cnt, t_id))
      return;
  }
  else return;

  while (p_ops->r_fifo->bcast_size > 0 &&  mes_sent < available_credits) {
    if (DEBUG_READS)
      printf("Wrkr %d has %u read bcasts to send credits %d\n",t_id, p_ops->r_fifo->bcast_size, credits[R_VC][0]);
    // Create the broadcast messages
    forge_r_wr(bcast_pull_ptr, p_ops, q_info, cb, r_send_sgl, r_send_wr, r_br_tx, br_i, credits, vc, t_id);
    br_i++;
    uint8_t coalesce_num = p_ops->r_fifo->r_message[bcast_pull_ptr].coalesce_num;
    if (ENABLE_ASSERTIONS) {
      assert( p_ops->r_fifo->bcast_size >= coalesce_num);
      (*outstanding_reads) += coalesce_num;
    }
    if (ENABLE_STAT_COUNTING) {
      t_stats[t_id].reads_sent += coalesce_num;
      t_stats[t_id].reads_sent_mes_num++;
    }
    // This message has been sent, do not add other reads to it!
    if (coalesce_num < MAX_R_COALESCE) {
      //yellow_printf("Broadcasting r_rep with coalesce num %u \n", coalesce_num);
      MOD_ADD(p_ops->r_fifo->push_ptr, R_FIFO_SIZE);
      p_ops->r_fifo->r_message[p_ops->r_fifo->push_ptr].coalesce_num = 0;
    }
    p_ops->r_fifo->bcast_size -= coalesce_num;
    reads_sent += coalesce_num;
    mes_sent++;
    MOD_ADD(bcast_pull_ptr, R_FIFO_SIZE);
    if (br_i == MAX_BCAST_BATCH) {
      post_quorum_broadasts_and_recvs(r_rep_recv_info, MAX_RECV_R_REP_WRS - r_rep_recv_info->posted_recvs,
                                      q_info, br_i, *r_br_tx, r_send_wr, cb->dgram_qp[R_QP_ID],
                                      R_ENABLE_INLINING);
      br_i = 0;
    }
  }
  if (br_i > 0)
    post_quorum_broadasts_and_recvs(r_rep_recv_info, MAX_RECV_R_REP_WRS - r_rep_recv_info->posted_recvs,
                                    q_info, br_i, *r_br_tx, r_send_wr, cb->dgram_qp[R_QP_ID],
                                    R_ENABLE_INLINING);
  p_ops->r_fifo->bcast_pull_ptr = bcast_pull_ptr;
  if (mes_sent > 0) decrease_credits(credits, q_info, mes_sent, vc);
}
/* ---------------------------------------------------------------------------
//------------------------------ POLLLING------- -----------------------------
//---------------------------------------------------------------------------*/

// Keep track of the write messages to send the appropriate acks
static inline void  ack_bookkeeping(struct ack_message *ack, uint8_t w_num, uint64_t l_id)
{
  if (ENABLE_ASSERTIONS && DEBUG_QUORUM) {
    if(unlikely(*(uint64_t *) ack->local_id) + ack->ack_num != l_id) {
      red_printf("Adding to existing ack with l_id %lu, ack_num %u with new l_id %lu and w_num %u\n",
                 *(uint64_t *) ack->local_id, ack->ack_num, l_id, w_num);
      //assert(false);
    }
  }
  if (ack->opcode == CACHE_OP_ACK) {// new ack
    //if (ENABLE_ASSERTIONS) assert((*(uint64_t *)ack->local_id) + ack->ack_num == l_id);
    memcpy(ack->local_id, &l_id, sizeof(uint64_t));
    ack->credits = 1;
    ack->ack_num = w_num;
    ack->opcode = ACK_NOT_YET_SENT;
    if (DEBUG_ACKS) yellow_printf("Create an ack with l_id  %lu \n", *(uint64_t *)ack->local_id);
  }
  else {
    if (ENABLE_ASSERTIONS) assert((*(uint64_t *)ack->local_id) + ack->ack_num == l_id);
    ack->credits++;
    if (ENABLE_ASSERTIONS) assert(ack->ack_num < 63000);
    ack->ack_num += w_num;
  }
}

// Wait until the entire write is there
static inline void wait_for_the_entire_write(volatile struct w_message *w_mes,
                                             uint16_t t_id, uint32_t index)
{
  uint32_t debug_cntr = 0;
  while (w_mes->write[w_mes->w_num - 1].opcode != CACHE_OP_PUT) {
    if (w_mes->write[w_mes->w_num - 1].opcode == OP_RELEASE) return;
    if (w_mes->write[w_mes->w_num - 1].opcode == OP_ACQUIRE) return;
    if (ENABLE_ASSERTIONS) {
      assert(false);
      debug_cntr++;
      if (debug_cntr == B_4_) {
        red_printf("Wrkr %d stuck waiting for a write to come index %u coalesce id %u\n",
                   t_id, index, w_mes->w_num - 1);
        print_wrkr_stats(t_id);
        debug_cntr = 0;
      }
    }
  }
}

// Poll for the write broadcasts
static inline void poll_for_writes(volatile struct w_message_ud_req *incoming_ws,
                                   uint32_t *pull_ptr, struct pending_ops *p_ops,
                                   struct ibv_cq *w_recv_cq, struct ibv_wc *w_recv_wc,
                                   struct recv_info *w_recv_info, struct ack_message *acks,
                                   uint16_t t_id)
{
  uint32_t polled_messages = 0, polled_writes = 0;
  int completed_messages =  ibv_poll_cq(w_recv_cq, W_BUF_SLOTS, w_recv_wc);
  if (completed_messages <= 0) return;
  uint32_t index = *pull_ptr;
  // Start polling
  while (polled_messages < completed_messages) {
    if (ENABLE_ASSERTIONS) {
      assert(incoming_ws[index].w_mes.w_num > 0);
      wait_for_the_entire_write(&incoming_ws[index].w_mes, t_id, index);
    }
    if (DEBUG_WRITES)
      printf("Worker sees a write Opcode %d at offset %d, l_id %lu  \n",
             incoming_ws[index].w_mes.write[0].opcode, index, *(uint64_t *)incoming_ws[index].w_mes.l_id);
    struct w_message *w_mes = (struct w_message*) &incoming_ws[index].w_mes;
    uint8_t w_num = w_mes->w_num;
    // Back-pressure
    if (ENABLE_ASSERTIONS && polled_writes + w_num > MAX_INCOMING_W) {
      assert(false);
      break;
    }
    for (uint16_t i = 0; i < w_num; i++) {
      struct write *write = &w_mes->write[i];
      if (ENABLE_ASSERTIONS) {
        if(write->opcode != CACHE_OP_PUT && write->opcode != OP_RELEASE && write->opcode != OP_ACQUIRE)
          red_printf("Receiving write : Opcode %u, i %u/%u \n", write->opcode, i, w_num);
        if ((*(uint32_t *)write->version) % 2 != 0) {
          red_printf("Odd version %u, m_id %u \n", *(uint32_t *)write->version, write->m_id);
        }
      }
      p_ops->ptrs_to_w_ops[polled_writes] = (struct write *)(((void *) write) - 3); // align with cache_op
      polled_writes++;
    }
    ack_bookkeeping(&acks[w_mes->m_id], w_num, *(uint64_t *)w_mes->l_id);
    if (ENABLE_STAT_COUNTING) {
      if (ENABLE_ASSERTIONS) t_stats[t_id].per_worker_writes_received[w_mes->m_id] += w_num;
      t_stats[t_id].received_writes += w_num;
      t_stats[t_id].received_writes_mes_num++;
    }
    if (ENABLE_ASSERTIONS) incoming_ws[index].w_mes.w_num = 0;
    MOD_ADD(index, W_BUF_SLOTS);
    polled_messages++;
  }
  (*pull_ptr) = index;

  if (polled_writes > 0) {
    if (DEBUG_WRITES) yellow_printf("Worker %u is going with %u writes to the cache \n", t_id, polled_writes);
    cache_batch_op_updates((uint32_t) polled_writes, 0, p_ops->ptrs_to_w_ops, 0, MAX_INCOMING_W, ENABLE_ASSERTIONS == 1);
    if (DEBUG_WRITES) yellow_printf("Worker %u propagated %u writes to the cache \n", t_id, polled_writes);
  }
}

// Wait until the entire r_rep is there
static inline void wait_for_the_entire_read(volatile struct r_message *r_mes,
                                             uint16_t t_id, uint32_t index)
{
  uint32_t debug_cntr = 0;
  while (r_mes->read[r_mes->coalesce_num - 1].opcode != CACHE_OP_GET) {
    if (r_mes->read[r_mes->coalesce_num - 1].opcode == CACHE_OP_LIN_PUT) return;
    if (r_mes->read[r_mes->coalesce_num - 1].opcode == OP_ACQUIRE) return;
    if (ENABLE_ASSERTIONS) {
      assert(false);
      debug_cntr++;
      if (debug_cntr == B_4_) {
        red_printf("Wrkr %d stuck waiting for a read to come index %u coalesce id %u\n",
                   t_id, index, r_mes->coalesce_num - 1);
        print_wrkr_stats(t_id);
        debug_cntr = 0;
      }
    }
  }
}

// Poll for the r_rep broadcasts
static inline void poll_for_reads(volatile struct r_message_ud_req *incoming_rs,
                                  uint32_t *pull_ptr, struct pending_ops *p_ops,
                                  struct ibv_cq *r_recv_cq, struct ibv_wc *r_recv_wc,
                                  struct recv_info *r_recv_info, struct ack_message *acks,
                                  uint16_t t_id, uint32_t *dbg_counter)
{
  if (p_ops->r_rep_fifo->mes_size == R_REP_FIFO_SIZE) return;
  int completed_messages =  ibv_poll_cq(r_recv_cq, R_BUF_SLOTS, r_recv_wc);
  if (completed_messages <= 0) return;
  uint32_t index = *pull_ptr;
  uint32_t polled_messages = 0, polled_reads = 0;
  // Start polling
  while (polled_messages < completed_messages) {
    if (ENABLE_ASSERTIONS) {
      assert(incoming_rs[index].r_mes.coalesce_num > 0);
      wait_for_the_entire_read(&incoming_rs[index].r_mes, t_id, index);
    }
    if (DEBUG_READS)
      printf("Worker sees a read Opcode %d at offset %d, l_id %lu  \n",
             incoming_rs[index].r_mes.read[0].opcode, index, *(uint64_t *)incoming_rs[index].r_mes.l_id);
    struct r_message *r_mes = (struct r_message*) &incoming_rs[index].r_mes;
    uint8_t r_num = r_mes->coalesce_num;

    if (polled_reads + r_num > MAX_INCOMING_R && ENABLE_ASSERTIONS) assert(false);
    for (uint16_t i = 0; i < r_num; i++) {
      struct read *read = &r_mes->read[i];
      if (ENABLE_ASSERTIONS) if(read->opcode != CACHE_OP_GET && read->opcode != OP_ACQUIRE)
          red_printf("Receiving read: Opcode %u, i %u/%u \n", read->opcode, i, r_num);
      //printf("version %u \n", *(uint32_t*) read->ts.version);
      p_ops->ptrs_to_r_ops[polled_reads] = (struct read *)(((void *) read) - 3); //align with the cache op
      p_ops->ptrs_to_r_headers[polled_reads] = r_mes;
      polled_reads++;
    }
    if (ENABLE_STAT_COUNTING) {
      if (ENABLE_ASSERTIONS) t_stats[t_id].per_worker_reads_received[r_mes->m_id] += r_num;
      t_stats[t_id].received_reads += r_num;
      t_stats[t_id].received_reads_mes_num++;
    }
    if (ENABLE_ASSERTIONS) incoming_rs[index].r_mes.coalesce_num = 0;
    MOD_ADD(index, R_BUF_SLOTS);
    polled_messages++;
    // Back-pressure
    if (polled_messages + p_ops->r_rep_fifo->mes_size == R_REP_FIFO_SIZE) {
      assert(false);
      break;
    }
  }
  (*pull_ptr) = index;
  // Poll for the completion of the receives
  if (polled_messages > 0) {
    cache_batch_op_reads(polled_reads, t_id, p_ops, 0, MAX_INCOMING_R, ENABLE_ASSERTIONS == 1);
    if (ENABLE_ASSERTIONS) dbg_counter[R_QP_ID] = 0;
  }
  else if (ENABLE_ASSERTIONS && p_ops->r_rep_fifo->mes_size == 0) dbg_counter[R_QP_ID]++;


}


// Form the  work request for the read reply
static inline void forge_r_rep_wr(uint32_t r_rep_i, uint16_t mes_i, struct pending_ops *p_ops,
                                  struct hrd_ctrl_blk *cb, struct ibv_sge *send_sgl,
                                  struct ibv_send_wr *send_wr, uint64_t *r_rep_tx,
                                  uint16_t t_id) {

  struct ibv_wc signal_send_wc;
  struct r_rep_fifo *r_rep_fifo = p_ops->r_rep_fifo;
  struct r_rep_message *r_rep_mes = &r_rep_fifo->r_rep_message[r_rep_i];
  uint16_t coalesce_num = r_rep_mes->coalesce_num;

  send_sgl[mes_i].length = r_rep_fifo->message_sizes[r_rep_i];
  send_sgl[mes_i].addr = (uint64_t) (uintptr_t) r_rep_mes;


  for (uint16_t i = 0; i < coalesce_num; i++) {
    if (DEBUG_READS)
      yellow_printf("Read Reply %d, message mes_size %d \n", i,
                    send_sgl[mes_i].length);
      }
  if (DEBUG_READS)
    green_printf("Wrkr %d : I send a READ REPLY message of %u r reps with mes_size %u, with lid: %u to machine %u \n",
                 t_id, coalesce_num, send_sgl[mes_i].length,
                 *(uint64_t*)r_rep_mes->l_id, r_rep_fifo->rem_m_id[r_rep_i]);
  if (ENABLE_ASSERTIONS) {
    assert(r_rep_fifo->rem_m_id[r_rep_i] < MACHINE_NUM);
    assert(coalesce_num > 0);
  }

  uint8_t rm_id = r_rep_fifo->rem_m_id[r_rep_i];
  if (ENABLE_ADAPTIVE_INLINING)
    adaptive_inlining(send_sgl[mes_i].length, &send_wr[mes_i], 1);
  else send_wr[mes_i].send_flags = R_REP_ENABLE_INLINING ? IBV_SEND_INLINE : 0;
  send_wr[mes_i].wr.ud.ah = remote_qp[rm_id][t_id][R_REP_QP_ID].ah;
  send_wr[mes_i].wr.ud.remote_qpn = (uint32) remote_qp[rm_id][t_id][R_REP_QP_ID].qpn;
  // Do a Signaled Send every R_SS_BATCH messages
  if ((*r_rep_tx) % R_REP_SS_BATCH == 0) send_wr[mes_i].send_flags |= IBV_SEND_SIGNALED;
  (*r_rep_tx)++;
  if ((*r_rep_tx) % R_REP_SS_BATCH == R_REP_SS_BATCH - 1) {
    //printf("Wrkr %u POLLING for a send completion in read replies \n", m_id);
    poll_cq(cb->dgram_send_cq[R_REP_QP_ID], 1, &signal_send_wc, POLL_CQ_R_REP);
  }
  if (mes_i > 0) send_wr[mes_i - 1].next = &send_wr[mes_i];

}

// Send Read Replies
static inline void send_r_reps(struct pending_ops *p_ops, struct hrd_ctrl_blk *cb,
                               struct ibv_send_wr *r_rep_send_wr, struct ibv_sge *r_rep_send_sgl,
                               struct recv_info *r_recv_info, uint64_t *r_rep_tx,  uint16_t t_id)
{
  uint16_t mes_i = 0, r_reps_sent = 0;//, credits_sent = 0;
  uint32_t pull_ptr = p_ops->r_rep_fifo->pull_ptr;
  struct ibv_send_wr *bad_send_wr;

  struct r_rep_fifo *r_rep_fifo = p_ops->r_rep_fifo;
  while (r_rep_fifo->total_size > 0) {
    if (DEBUG_READS)
      printf("Wrkr %d has %u read replies to send \n", t_id, r_rep_fifo->total_size);
    // Create the r_rep messages
    forge_r_rep_wr(pull_ptr, mes_i, p_ops, cb, r_rep_send_sgl, r_rep_send_wr, r_rep_tx, t_id);
    uint8_t coalesce_num = r_rep_fifo->r_rep_message[pull_ptr].coalesce_num;
    if (ENABLE_ASSERTIONS) {
      assert(r_rep_fifo->total_size >= coalesce_num);
    }
    if (ENABLE_STAT_COUNTING) {
      t_stats[t_id].r_reps_sent += coalesce_num;
      t_stats[t_id].r_reps_sent_mes_num++;
    }
    r_rep_fifo->total_size -= coalesce_num;
    r_rep_fifo->mes_size--;
    r_reps_sent += coalesce_num;
    //credits_sent += r_rep_fifo->r_rep_message[pull_ptr].credits;
    MOD_ADD(pull_ptr, R_REP_FIFO_SIZE);
    if (ENABLE_ASSERTIONS) assert(mes_i < MAX_R_REP_WRS);
    mes_i++;
  }
  if (mes_i > 0) {
    if (DEBUG_READS) printf("Wrkr %d posting %d read recvs\n", t_id,  mes_i);
    post_recvs_with_recv_info(r_recv_info, mes_i);
    r_rep_send_wr[mes_i - 1].next = NULL;
    int ret = ibv_post_send(cb->dgram_qp[R_REP_QP_ID], &r_rep_send_wr[0], &bad_send_wr);
    if (ENABLE_ASSERTIONS) CPE(ret, "R_REP ibv_post_send error", ret);
  }
  r_rep_fifo->pull_ptr = pull_ptr;

}

// Each read has an associated read_info structure that keeps track of the incoming replies, value, opcode etc.
static inline void read_info_bookkeeping(struct r_rep_big *r_rep, struct read_info * read_info)
{
  if (r_rep->opcode == TS_GREATER || r_rep->opcode == TS_GREATER_LIN_PUT) {
    if (ENABLE_ASSERTIONS) {
      if (r_rep->opcode == TS_GREATER_LIN_PUT) assert(read_info->opcode == CACHE_OP_LIN_PUT);
      else assert(read_info->opcode != CACHE_OP_LIN_PUT);
    }
    if (!read_info->seen_larger_ts) { // If this is the first "Greater" ts
      read_info->ts_to_read = r_rep->ts;
      read_info->times_seen_ts = 1;
      memcpy(read_info->value, r_rep->value, VALUE_SIZE);
      read_info->seen_larger_ts = true;
    }
    else { // if the read has already received a "greater" ts
      enum ts_compare ts_comp = compare_ts(&read_info->ts_to_read, &r_rep->ts);
      if (ts_comp == SMALLER) {
        read_info->ts_to_read = r_rep->ts;
        read_info->times_seen_ts = 1;
        memcpy(read_info->value, r_rep->value, VALUE_SIZE);
      }
      if (ts_comp == EQUAL) read_info->times_seen_ts++;
      // Nothing to do if the already stored ts is greater than the incoming
    }
  }
  else if (r_rep->opcode == TS_EQUAL) {
    if (!read_info->seen_larger_ts)  // If it has not seen a "greater ts"
      read_info->times_seen_ts++;
    // Nothing to do if the already stored ts is greater than the incoming
  }
  else if (r_rep->opcode == TS_SMALLER) { // Nothing to do if the already stored ts is greater than the incoming

  }
  // assert(read_info->rep_num == 0);
  read_info->rep_num++;
}

// spin waiting for the r_rep
static inline void wait_until_the_entire_r_rep(volatile struct r_rep_big *stuck_r_rep, volatile struct r_rep_message_ud_req *r_rep_buf,
                                               uint16_t r_rep_i, uint32_t r_rep_pull_ptr, struct pending_ops *p_ops, uint16_t stuck_byte_ptr,
                                               uint16_t t_id)
{
  uint32_t debug_cntr = 0;
  while (stuck_r_rep->opcode < TS_SMALLER || stuck_r_rep->opcode > TS_GREATER) {
    if (ENABLE_ASSERTIONS) {
      //assert(false);
      debug_cntr++;
      if (debug_cntr == M_512_) {
        red_printf("Wrkr %d stuck waiting for a r_rep_mes to come, stuck opcode %u, stuck byte_ptr %u, r_rep_i %u, stuck ptr %lu  \n",
                   t_id, stuck_r_rep->opcode, stuck_byte_ptr, r_rep_i, stuck_r_rep);
        r_rep_pull_ptr = (r_rep_pull_ptr - 1) % R_REP_BUF_SLOTS;
        volatile struct r_rep_message *r_rep_mes = &r_rep_buf[r_rep_pull_ptr].r_rep_mes;
        uint64_t l_id = *(uint64_t *) r_rep_mes->l_id;
        uint8_t message_opc = r_rep_mes->opcode;
        cyan_printf("Wrkr %d, polling on index %u, polled opc %u, 1st r_rep_mes opcode: %u, l_id %lu, expected l_id %lu\n",
                    t_id, r_rep_pull_ptr, message_opc, r_rep_mes->opcode, l_id, p_ops->local_r_id);

        uint16_t byte_ptr = R_REP_MES_HEADER;
        for (uint16_t i = 0; i < r_rep_mes->coalesce_num; i++) {
          struct r_rep_big *r_rep = (struct r_rep_big *) (((void *) r_rep_mes) + byte_ptr);
          yellow_printf("R_rep %u/%u, opcode %u, byte_ptr %u , ptr %lu/%lu\n", i, r_rep_mes->coalesce_num, r_rep->opcode, byte_ptr,
                        r_rep, stuck_r_rep);
          if (r_rep->opcode == TS_GREATER) byte_ptr += R_REP_SIZE;
          else if (r_rep->opcode == TS_GREATER_LIN_PUT) byte_ptr += R_REP_LIN_PUT_SIZE;
          else byte_ptr++;
        }
        for (int i = 0; i < R_REP_BUF_SLOTS; ++i) {
          if (r_rep_buf[i].r_rep_mes.opcode == READ_REPLY) {
            green_printf("GOOD OPCODE in index %d, l_id %u \n", i, *(uint64_t *) r_rep_buf[i].r_rep_mes.l_id);
          } else
            red_printf("BAD OPCODE in index %d, l_id %u, from machine: %u  \n", i,
                       *(uint64_t *) r_rep_buf[i].r_rep_mes.l_id,
                       r_rep_buf[i].r_rep_mes.m_id);
        }
        print_wrkr_stats(t_id);
        debug_cntr = 0;
      }
    }
  }
}

//Poll for read replies
static inline void poll_for_read_replies(volatile struct r_rep_message_ud_req *incoming_r_reps,
                                         uint32_t *pull_ptr, struct pending_ops *p_ops,
                                         uint16_t credits[][MACHINE_NUM],
                                         struct ibv_cq *r_rep_recv_cq, struct ibv_wc *r_rep_recv_wc,
                                         struct recv_info *r_rep_recv_info, uint16_t t_id,
                                         uint32_t *outstanding_reads, uint32_t *debug_cntr)
{
  if (p_ops->r_rep_fifo->mes_size == R_REP_FIFO_SIZE) return;
  int completed_messages =  ibv_poll_cq(r_rep_recv_cq, R_REP_BUF_SLOTS, r_rep_recv_wc);
  if (completed_messages <= 0) return;
  uint32_t index = *pull_ptr;
  uint32_t polled_messages = 0, polled_r_reps = 0;
  // Start polling
  while (polled_messages < completed_messages) {
  //while (incoming_r_reps[index].r_rep_mes.opcode == READ_REPLY) {
    if (ENABLE_ASSERTIONS) assert(incoming_r_reps[index].r_rep_mes.opcode == READ_REPLY);
    //wait_for_the_entire_read(&incoming_rs[index].r_mes, t_id, index);
    if (DEBUG_READS)
      yellow_printf("Worker sees a READ REPLY: %d at offset %d, l_id %lu, from machine %u with %u replies\n",
             incoming_r_reps[index].r_rep_mes.opcode, index,
             *(uint64_t *)incoming_r_reps[index].r_rep_mes.l_id,
                    incoming_r_reps[index].r_rep_mes.m_id,
                    incoming_r_reps[index].r_rep_mes.coalesce_num);
    struct r_rep_message *r_rep_mes = (struct r_rep_message*) &incoming_r_reps[index].r_rep_mes;
    uint8_t r_rep_num = r_rep_mes->coalesce_num;
    polled_messages++;
    r_rep_mes->opcode = 5; // a random meaningless opcode
    MOD_ADD(index, R_REP_BUF_SLOTS);
    // Find the request that the reply is referring to
    uint64_t l_id = *(uint64_t *) (r_rep_mes->l_id);
    uint64_t pull_lid = p_ops->local_r_id; // l_id at the pull pointer
    uint32_t r_ptr; // a pointer in the FIFO, from where r_rep refers to
    credits[R_VC][r_rep_mes->m_id]++;
    if (ENABLE_ASSERTIONS) {
      assert(r_rep_mes->m_id < MACHINE_NUM);
      assert(r_rep_mes->coalesce_num > 0);
      assert(credits[R_VC][r_rep_mes->m_id] <= R_CREDITS);
    }
    // if the pending read FIFO is empty it means the r_reps are for committed messages.
    if (p_ops->r_size == 0 ) {
      if (!USE_QUORUM) assert(false);
      continue;
    }
    if (ENABLE_ASSERTIONS) {
      assert(l_id + r_rep_num <= pull_lid + p_ops->r_size);
      if ((l_id + r_rep_num < pull_lid) && (!USE_QUORUM)) {
        red_printf("l_id %u, r_rep_num %u, pull_lid %u \n", l_id, r_rep_num, pull_lid);
        exit(0);
      }
    }

    if (pull_lid >= l_id) {
      if ((pull_lid - l_id) >= r_rep_num) continue;
      r_ptr = p_ops->r_pull_ptr;
    }
    else  // l_id > pull_lid
      r_ptr = (uint32_t) (p_ops->r_pull_ptr + (l_id - pull_lid)) % PENDING_READS;

    uint16_t byte_ptr = R_REP_MES_HEADER;
    for (uint16_t i = 0; i < r_rep_num; i++) {
      struct r_rep_big *r_rep = (struct r_rep_big *)(((void *)r_rep_mes) + byte_ptr);
      if (ENABLE_ASSERTIONS) {
        if (r_rep->opcode < TS_SMALLER || r_rep->opcode > TS_GREATER) {
          wait_until_the_entire_r_rep((volatile struct r_rep_big *) r_rep, incoming_r_reps, i,
                                      index, p_ops, byte_ptr, t_id);
          //red_printf("Receiving r_rep: Opcode %u, i %u/%u \n", r_rep->opcode, i, r_rep_num);
        }
      }
      if (r_rep->opcode == TS_GREATER) byte_ptr += R_REP_SIZE;
      else if (r_rep->opcode == TS_GREATER_LIN_PUT) byte_ptr += R_REP_LIN_PUT_SIZE;
      else byte_ptr++;
      polled_r_reps++;
      if (pull_lid >= l_id) {
        if (l_id + i < pull_lid) continue;
      }
      struct read_info *read_info = &p_ops->read_info[r_ptr];
      read_info_bookkeeping(r_rep, read_info);
      if (DEBUG_READS)
        yellow_printf("Read reply %u, Received replies %u/%d at r_ptr %u \n",
                      i, read_info->rep_num, REMOTE_QUORUM, r_ptr);
      if (read_info->rep_num >= REMOTE_QUORUM) {
        // if (ENABLE_ASSERTIONS) assert(p_ops->r_state[r_ptr] >= SENT);
        p_ops->r_state[r_ptr] = READY;
        if (ENABLE_ASSERTIONS) {
          (*outstanding_reads)--;
          assert(read_info->rep_num <= REM_MACH_NUM);
        }
      }
      r_ptr = (r_ptr + 1) % PENDING_READS;
      r_rep->opcode = 5;
    }
    if (ENABLE_STAT_COUNTING) {
      if (ENABLE_ASSERTIONS) t_stats[t_id].per_worker_r_reps_received[r_rep_mes->m_id] += r_rep_num;
      t_stats[t_id].received_r_reps += r_rep_num;
      t_stats[t_id].received_r_reps_mes_num++;
    }
  }
  (*pull_ptr) = index;
  // Poll for the completion of the receives
  if (polled_messages > 0) {
    if (ENABLE_ASSERTIONS) assert(r_rep_recv_info->posted_recvs >= polled_messages);
    r_rep_recv_info->posted_recvs -= polled_messages;
    //poll_cq(r_rep_recv_cq, polled_messages, r_rep_recv_wc);
    if (ENABLE_ASSERTIONS) debug_cntr[R_REP_QP_ID] = 0;
  }
  else {
    if (ENABLE_ASSERTIONS && (*outstanding_reads) > 0) debug_cntr[R_REP_QP_ID]++;
    if (ENABLE_STAT_COUNTING && (*outstanding_reads) > 0) t_stats[t_id].stalled_r_rep++;
  }
}

static inline void commit_reads(struct pending_ops *p_ops, uint16_t t_id)
{
  uint32_t pull_ptr = p_ops->r_pull_ptr;
  uint16_t writes_for_cache = 0;

  while(p_ops->r_state[pull_ptr] == READY) {
    if (p_ops->read_info[pull_ptr].times_seen_ts >= REMOTE_QUORUM &&
      (p_ops->read_info[pull_ptr].opcode != CACHE_OP_LIN_PUT)) {
      if (ENABLE_ASSERTIONS)
        assert(p_ops->read_info[pull_ptr].opcode == OP_ACQUIRE);
      if (DEBUG_READS || DEBUG_TS)
        green_printf("Committing read at index %u, it has seen %u times the same timestamp\n",
                     pull_ptr, p_ops->read_info[pull_ptr].times_seen_ts);
      // commit this read
      p_ops->r_state[pull_ptr] = INVALID;
      if (p_ops->read_info[pull_ptr].opcode == OP_ACQUIRE) {
        p_ops->session_has_pending_op[p_ops->r_session_id[pull_ptr]] = false;
        p_ops->all_sessions_stalled = false;
      }
      memset(&p_ops->read_info[pull_ptr], 0, 3);
      MOD_ADD(pull_ptr, PENDING_READS);
      p_ops->r_size--;
      p_ops->local_r_id++;
    }
    else if (p_ops->w_size + writes_for_cache < PENDING_WRITES && writes_for_cache < MAX_INCOMING_R) {
      if (DEBUG_READS || DEBUG_TS)
        green_printf("Seen larger ts: at  %u, it has seen %u times the same timestamp\n",
                     pull_ptr, p_ops->read_info[pull_ptr].times_seen_ts);
      if (ENABLE_LIN && p_ops->read_info[pull_ptr].opcode == CACHE_OP_LIN_PUT) {
//        printf("Creating a lin put ptr to send to the cache %u\n", lin_puts_num);
        p_ops->read_info[pull_ptr].ts_to_read.m_id = (uint8_t) machine_id;
        *(uint32_t *)p_ops->read_info[pull_ptr].ts_to_read.version += 2;
        uint32_t session_id = p_ops->r_session_id[pull_ptr];
        memcpy(&p_ops->read_info[pull_ptr], &session_id, SESSION_BYTES);
        insert_write(p_ops, (struct cache_op *) &p_ops->read_info[pull_ptr], LIN_WRITE, 0, t_id);
        p_ops->ptrs_to_r_ops[writes_for_cache] = (struct read *) &p_ops->read_info[pull_ptr];
        writes_for_cache++;
      }
      else { // convert the read to a write and push it to the write ops
        if (ENABLE_STAT_COUNTING) t_stats[t_id].read_to_write++;
        insert_write(p_ops, NULL, FROM_READ, pull_ptr, t_id);
        memset(&p_ops->read_info[pull_ptr], 0, 3);
        if (ENABLE_ASSERTIONS)
          assert(p_ops->read_info[pull_ptr].opcode == CACHE_OP_GET ||
                 p_ops->read_info[pull_ptr].opcode == OP_ACQUIRE);
        if (p_ops->read_info[pull_ptr].seen_larger_ts) { // then we also have to propagate this read to the cache
          p_ops->ptrs_to_r_ops[writes_for_cache] = (struct read *) &p_ops->read_info[pull_ptr];
          writes_for_cache++;
        }
      }
      p_ops->r_state[pull_ptr] = INVALID;
      MOD_ADD(pull_ptr, PENDING_READS);
      p_ops->r_size--;
      p_ops->local_r_id++;
    }
    else break;
  }
  p_ops->r_pull_ptr = pull_ptr;
  if (writes_for_cache > 0)
    cache_batch_op_lin_writes_and_unseen_reads(writes_for_cache, t_id, (struct read_info **) p_ops->ptrs_to_r_ops,
                                                 0, MAX_INCOMING_R, false);

  //printf("polling acks 6\n");
}


// Send a batched ack that denotes the first local write id and the number of subsequent lids that are being acked
static inline void send_acks(struct ibv_send_wr *ack_send_wr,
                             uint64_t *sent_ack_tx,
                             struct hrd_ctrl_blk *cb, struct recv_info *w_recv_info,
                             struct ack_message *acks, uint16_t t_id)
{
  uint8_t ack_i = 0, prev_ack_i = 0, first_wr = 0;
  struct ibv_wc signal_send_wc;
  struct ibv_send_wr *bad_send_wr;
  uint32_t recvs_to_post_num = 0;

  for (uint8_t i = 0; i < MACHINE_NUM; i++) {
    if (acks[i].opcode == CACHE_OP_ACK) continue;
    if (ENABLE_STAT_COUNTING) {
      t_stats[t_id].per_worker_acks_sent[i] += acks[i].ack_num;
      t_stats[t_id].per_worker_acks_mes_sent[i]++;
      t_stats[t_id].acks_sent += acks[i].ack_num;
      t_stats[t_id].acks_sent_mes_num++;
    }
    if (DEBUG_ACKS)
      yellow_printf("Wrkr %d is sending an ack for lid %lu, credits %u and ack num %d and m id %d \n",
                    t_id, *(uint64_t *)acks[i].local_id, acks[i].credits, acks[i].ack_num, acks[i].m_id);

    acks[i].opcode = CACHE_OP_ACK;
    if (ENABLE_ASSERTIONS) {
      assert(acks[i].credits <= acks[i].ack_num);
      if (acks[i].ack_num >= MAX_W_COALESCE) assert(acks[i].credits > 0);
      assert(acks[i].ack_num > 0);
    }
    if ((*sent_ack_tx) % ACK_SS_BATCH == 0) {
      ack_send_wr[i].send_flags |= IBV_SEND_SIGNALED;
      // if (t_id == 0) green_printf("Sending ack %llu signaled \n", *sent_ack_tx);
    } else ack_send_wr[i].send_flags = IBV_SEND_INLINE;
    if ((*sent_ack_tx) % ACK_SS_BATCH == ACK_SS_BATCH - 1) {
      // if (t_id == 0) green_printf("Polling for ack  %llu \n", *sent_ack_tx);
      poll_cq(cb->dgram_send_cq[ACK_QP_ID], 1, &signal_send_wc, POLL_CQ_ACK);
    }
    if (ack_i > 0) {
      if (DEBUG_ACKS) yellow_printf("Wrkr %u, ack %u points to ack %u \n", t_id, prev_ack_i, i);
      ack_send_wr[prev_ack_i].next = &ack_send_wr[i];
    }
    else first_wr = i;
    (*sent_ack_tx)++; // Selective signaling
    recvs_to_post_num += acks[i].credits;
    ack_i++;
    prev_ack_i = i;
  }
  // RECEIVES for writes
  if (recvs_to_post_num > 0) {
    post_recvs_with_recv_info(w_recv_info, recvs_to_post_num);
    //w_recv_info->posted_recvs += recvs_to_post_num;
       // printf("Wrkr %d posting %u recvs and has a total of %u recvs for writes \n",
        //       t_id, recvs_to_post_num,  w_recv_info->posted_recvs);
    if (ENABLE_ASSERTIONS) {
      assert(recvs_to_post_num <= MAX_RECV_W_WRS);
      //assert(w_recv_info->posted_recvs <= MAX_RECV_W_WRS);
    }
  }
  // SEND the acks
  if (ack_i > 0) {
    if (DEBUG_ACKS) printf("send %u acks, last recipient %u, first recipient %u \n", ack_i, prev_ack_i, first_wr);
    ack_send_wr[prev_ack_i].next = NULL;
    int ret = ibv_post_send(cb->dgram_qp[ACK_QP_ID], &ack_send_wr[first_wr], &bad_send_wr);
    if (ENABLE_ASSERTIONS) CPE(ret, "ACK ibv_post_send error", ret);
  }
}


// Spin until you know the entire message is there
static inline void wait_for_the_entire_ack(volatile struct ack_message *ack,
                                           uint16_t t_id, uint32_t index)
{
  uint32_t debug_cntr = 0;
  while (ack->ack_num  == 0) {
    assert(false);
    if (ENABLE_ASSERTIONS) {
      debug_cntr++;
      if (debug_cntr > M_128) {
        red_printf("Wrkr %d stuck waiting for an ack to come index %u ack_num %u\n",
                   t_id, index, ack->ack_num);
        print_wrkr_stats(t_id);
        debug_cntr = 0;
      }
    }
  }
}

// Remove writes that have seen all acks
static inline void remove_writes(struct pending_ops *p_ops, uint16_t t_id)
{
  while(p_ops->w_state[p_ops->w_pull_ptr] >= READY_PUT) {
    //if (DEBUG_ACKS)
    //  green_printf("Wkrk %u freeing write at pull_ptr %u, w_size %u, w_state %d, session %u, local_w_id %lu, acks seen %u \n",
    //               t_id, p_ops->w_pull_ptr, p_ops->w_size, p_ops->w_state[p_ops->w_pull_ptr],
    //               p_ops->w_session_id[p_ops->w_pull_ptr], p_ops->local_w_id, p_ops->acks_seen[p_ops->w_pull_ptr]);
    if (p_ops->w_state[p_ops->w_pull_ptr] == READY_RELEASE) {
      p_ops->session_has_pending_op[p_ops->w_session_id[p_ops->w_pull_ptr]] = false;
      p_ops->all_sessions_stalled = false;
    }
    p_ops->w_state[p_ops->w_pull_ptr] = INVALID;
    p_ops->acks_seen[p_ops->w_pull_ptr] = 0;
    p_ops->local_w_id++;
    MOD_ADD(p_ops->w_pull_ptr, PENDING_WRITES);
    p_ops->w_size--;
  }
  if (ENABLE_ASSERTIONS) {
    assert(p_ops->w_state[p_ops->w_pull_ptr] < READY_PUT);
    assert(p_ops->w_state[(p_ops->w_pull_ptr + 1) % PENDING_WRITES] < READY_PUT);
  }
}


// Worker polls for acks
static inline void poll_acks(struct ack_message_ud_req *incoming_acks, uint32_t *pull_ptr,
                                 struct pending_ops *p_ops,
                                 uint16_t credits[][MACHINE_NUM],
                                 struct ibv_cq * ack_recv_cq, struct ibv_wc *ack_recv_wc,
                                 struct recv_info *ack_recv_info,
                                 uint16_t t_id, uint32_t *dbg_counter,
                                 uint32_t *outstanding_writes)
{
  uint32_t index = *pull_ptr;
  uint32_t polled_messages = 0;
  int completed_messages =  ibv_poll_cq(ack_recv_cq, ACK_BUF_SLOTS, ack_recv_wc);
  if (completed_messages <= 0) return;
  while (polled_messages < completed_messages) {
    struct ack_message *ack = &incoming_acks[index].ack;
    uint16_t ack_num = ack->ack_num;
    if (ENABLE_ASSERTIONS) {
      assert(incoming_acks[index].ack.opcode == CACHE_OP_ACK);
      wait_for_the_entire_ack((volatile struct ack_message *)ack, t_id, index);
      assert(ack->m_id < MACHINE_NUM);
    }
    MOD_ADD(index, ACK_BUF_SLOTS);
    polled_messages++;

    uint64_t l_id = *(uint64_t *) (ack->local_id);
    uint64_t pull_lid = p_ops->local_w_id; // l_id at the pull pointer
    uint32_t ack_ptr; // a pointer in the FIFO, from where ack should be added
    if (DEBUG_ACKS)
      yellow_printf("Wrkr %d  polled ack opcode %d with %d acks for l_id %lu, oldest lid %lu, at offset %d from machine %u \n",
                    t_id, ack->opcode, ack_num, l_id, pull_lid, index, ack->m_id);
    if (ENABLE_STAT_COUNTING) {
      if (ENABLE_ASSERTIONS) {
        t_stats[t_id].per_worker_acks_received[ack->m_id] += ack_num;
        t_stats[t_id].per_worker_acks_mes_received[ack->m_id]++;
      }
      t_stats[t_id].received_acks += ack_num;
      t_stats[t_id].received_acks_mes_num++;
    }
    credits[W_VC][ack->m_id] += ack->credits;
    // if the pending write FIFO is empty it means the acks are for committed messages.
    if (p_ops->w_size == 0 ) {
      if (!USE_QUORUM) assert(false);
      ack->opcode = 5;
      ack->ack_num = 0; continue;
    }
    if (ENABLE_ASSERTIONS) {
      assert(l_id + ack_num <= pull_lid + p_ops->w_size);
      if ((l_id + ack_num < pull_lid) && (!USE_QUORUM)) {
        red_printf("l_id %u, ack_num %u, pull_lid %u \n", l_id, ack_num, pull_lid);
        exit(0);
      }
    }
    if (pull_lid >= l_id) {
      if ((pull_lid - l_id) >= ack_num) {ack->opcode = 5;
        ack->ack_num = 0; continue;}//memset((void*)ack, 0, ACK_SIZE); continue;}
      ack_num -= (pull_lid - l_id);
      // if (ENABLE_ASSERTIONS) assert(ack_num > 0 && ack_num <= FLR_PENDING_WRITES);
      ack_ptr = p_ops->w_pull_ptr;
    }
    else { // l_id > pull_lid
      ack_ptr = (uint32_t) (p_ops->w_pull_ptr + (l_id - pull_lid)) % PENDING_WRITES;
      //if (ENABLE_ASSERTIONS) assert(false);
    }
    // Apply the acks that refer to stored writes
    for (uint16_t ack_i = 0; ack_i < ack_num; ack_i++) {
      if (ENABLE_ASSERTIONS && (ack_ptr == p_ops->w_push_ptr)) {
        uint32_t origin_ack_ptr = (ack_ptr - ack_i + PENDING_WRITES) % PENDING_WRITES;
        red_printf("Origin ack_ptr %u/%u, acks %u/%u, w_pull_ptr %u, w_push_ptr % u, w_size %u \n",
                   origin_ack_ptr,  (p_ops->w_pull_ptr + (l_id - pull_lid)) % PENDING_WRITES,
                   ack_i, ack_num, p_ops->w_pull_ptr, p_ops->w_push_ptr, p_ops->w_size);
      }
      p_ops->acks_seen[ack_ptr]++;
      if (ENABLE_ASSERTIONS) assert(p_ops->acks_seen[ack_ptr] <= REM_MACH_NUM);
      if (p_ops->acks_seen[ack_ptr] == REMOTE_QUORUM) {
        if (ENABLE_ASSERTIONS) (*outstanding_writes)--;
        //printf("Wrkr %d valid ack %u/%u, write at ptr %d is ready \n",
            //   t_id, ack_i, ack_num,  ack_ptr);
        if (p_ops->w_state[ack_ptr] == SENT_PUT) p_ops->w_state[ack_ptr] = READY_PUT;
        else p_ops->w_state[ack_ptr] = READY_RELEASE;
      }
      MOD_ADD(ack_ptr, PENDING_WRITES);
    }
    if (ENABLE_ASSERTIONS) assert(credits[W_VC][ack->m_id] <= W_CREDITS);
    //memset((void*)ack, 0, ACK_SIZE); // need to delete all the g_ids
    ack->opcode = 5;
    ack->ack_num = 0;
  } // while

  *pull_ptr = index;
  // Poll for the completion of the receives
  if (polled_messages > 0) {
    remove_writes(p_ops, t_id);
    if (ENABLE_ASSERTIONS) dbg_counter[ACK_QP_ID] = 0;
  }
  else {
    if (ENABLE_ASSERTIONS && (*outstanding_writes) > 0) dbg_counter[ACK_QP_ID]++;
    if (ENABLE_STAT_COUNTING && (*outstanding_writes) > 0) t_stats[t_id].stalled_ack++;
  }
  if (ENABLE_ASSERTIONS) assert(ack_recv_info->posted_recvs >= polled_messages);
  ack_recv_info->posted_recvs -= polled_messages;
}


#endif /* INLINE_UTILS_H */
