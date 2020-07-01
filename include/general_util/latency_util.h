//
// Created by vasilis on 22/05/20.
//

#ifndef KITE_LATENCY_UTIL_H
#define KITE_LATENCY_UTIL_H

#include "top.h"
/* ---------------------------------------------------------------------------
//------------------------------ LATENCY MEASUREMENTS-------------------------
//---------------------------------------------------------------------------*/

//Add latency to histogram (in microseconds)
static inline void bookkeep_latency(int useconds, req_type rt){
  uint32_t** latency_counter;
  switch (rt){
    case ACQUIRE:
      latency_counter = &latency_count.acquires;
      if (useconds > latency_count.max_acq_lat) {
        latency_count.max_acq_lat = (uint32_t) useconds;
        //my_printf(green, "Found max acq latency: %u/%d \n",
        //             latency_count.max_acq_lat, useconds);
      }
      break;
    case RELEASE:
      latency_counter = &latency_count.releases;
      if (useconds > latency_count.max_rel_lat) {
        latency_count.max_rel_lat = (uint32_t) useconds;
        //my_printf(yellow, "Found max rel latency: %u/%d \n", latency_count.max_rel_lat, useconds);
      }
      break;
    case READ_REQ:
      latency_counter = &latency_count.hot_reads;
      if (useconds > latency_count.max_read_lat) latency_count.max_read_lat = (uint32_t) useconds;
      break;
    case WRITE_REQ:
      latency_counter = &latency_count.hot_writes;
      if (useconds > latency_count.max_write_lat) latency_count.max_write_lat = (uint32_t) useconds;
      break;
    default: assert(0);
  }
  latency_count.total_measurements++;

  if (useconds > MAX_LATENCY)
    (*latency_counter)[LATENCY_BUCKETS]++;
  else
    (*latency_counter)[useconds / (MAX_LATENCY / LATENCY_BUCKETS)]++;
}

//
static inline void report_latency(latency_info_t* latency_info)
{
  struct timespec end;
  clock_gettime(CLOCK_MONOTONIC, &end);
  int useconds = ((end.tv_sec - latency_info->start.tv_sec) * MILLION) +
                 ((end.tv_nsec - latency_info->start.tv_nsec) / 1000);  //(end.tv_nsec - start->tv_nsec) / 1000;
  if (ENABLE_ASSERTIONS) assert(useconds > 0);
  //if (useconds > 1000)
//    printf("Latency of a req of type %s is %u us, sess %u , thread reqs/ measured reqs: %ld \n",
//           latency_info->measured_req_flag == RELEASE ? "RELEASE" : "ACQUIRE",
//           useconds, latency_info->measured_sess_id,
//           t_stats[0].cache_hits_per_thread / (latency_count.total_measurements + 1));
  bookkeep_latency(useconds, latency_info->measured_req_flag);
  (latency_info->measured_req_flag) = NO_REQ;
}

// Necessary bookkeeping to initiate the latency measurement
static inline void start_measurement(latency_info_t* latency_info, uint32_t sess_id,
                                     uint16_t t_id, uint8_t opcode) {
  uint8_t compare_op = MEASURE_READ_LATENCY ? OP_ACQUIRE : OP_RELEASE ;
  if ((latency_info->measured_req_flag) == NO_REQ) {
    if (t_stats[t_id].cache_hits_per_thread > M_1 &&
        (MEASURE_READ_LATENCY == 2 || opcode == compare_op) &&
        t_id == LATENCY_THREAD && machine_id == LATENCY_MACHINE) {
      //printf("tag a key for latency measurement \n");
      //if (opcode == KVS_OP_GET) latency_info->measured_req_flag = HOT_READ_REQ;
      // else if (opcode == KVS_OP_PUT) {
      //  latency_info->measured_req_flag = HOT_WRITE_REQ;
      //}
      //else
      if (opcode == OP_RELEASE)
        latency_info->measured_req_flag = RELEASE;
      else if (opcode == OP_ACQUIRE) latency_info->measured_req_flag = ACQUIRE;
      else if (ENABLE_ASSERTIONS) assert(false);
      //my_printf(green, "Measuring a req %llu, opcode %d, flag %d op_i %d \n",
      //					 t_stats[t_id].cache_hits_per_thread, opcode, latency_info->measured_req_flag, latency_info->measured_sess_id);
      latency_info->measured_sess_id = sess_id;
      clock_gettime(CLOCK_MONOTONIC, &latency_info->start);
      if (ENABLE_ASSERTIONS) assert(latency_info->measured_req_flag != NO_REQ);
    }
  }
}

// A condition to be used to trigger periodic (but rare) measurements
static inline bool trigger_measurement(uint16_t local_client_id)
{
  return t_stats[local_client_id].cache_hits_per_thread % K_32 > 0 &&
         t_stats[local_client_id].cache_hits_per_thread % K_32 <= 500 &&
         local_client_id == 0 && machine_id == MACHINE_NUM -1;
}



// The follower sends a write to the leader and tags it with a session id. When the leaders sends a prepare,
// it includes that session id and a flr id
// the follower inspects the flr id, such that it can unblock the session id, if the write originated locally
// we hijack that connection for the latency, remembering the session that gets stuck on a write
static inline void change_latency_tag(latency_info_t *latency_info, uint32_t sess_id,
                                      uint16_t t_id)
{
  if (latency_info->measured_req_flag == WRITE_REQ_BEFORE_CACHE &&
      machine_id == LATENCY_MACHINE && t_id == LATENCY_THREAD &&
      latency_info->measured_sess_id == sess_id)
    latency_info-> measured_req_flag = WRITE_REQ;
}


#endif //KITE_LATENCY_UTIL_H
