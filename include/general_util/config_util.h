//
// Created by vasilis on 03/07/20.
//

#ifndef CONFIG_UTIL_H
#define CONFIG_UTIL_H

#include "generic_inline_util.h"
#include "debug_util.h"


/* ---------------------------------------------------------------------------
//------------------------------CONFIGURATION -----------------------------
//---------------------------------------------------------------------------*/
// Update the quorum info, use this one a timeout
// On a timeout it goes through all machines
static inline void update_q_info(quorum_info_t *q_info,  uint16_t *credits,
                                 uint16_t min_credits, uint16_t t_id)
{
  uint8_t i, rm_id;
  q_info->missing_num = 0;
  q_info->active_num = 0;
  for (i = 0; i < MACHINE_NUM; i++) {
    if (i == machine_id) continue;
    rm_id = mid_to_rmid(i);
    if (credits[i] < min_credits) {
      q_info->missing_ids[q_info->missing_num] = i;
      q_info->missing_num++;
      q_info->send_vector[rm_id] = false;
      //Change the machine-wide configuration bit-vector and the bit vector to be sent
      //set_conf_bit_after_detecting_failure(t_id, i); // this function changes both vectors
      //if (DEBUG_QUORUM) my_printf(yellow, "Worker flips the vector bit_vec for machine %u, send vector bit_vec %u \n",
      //                               i, send_bit_vector.bit_vec[i].bit);
      if (t_id == 0)
        my_printf(cyan, "Wrkr %u detects that machine %u has failed \n", t_id, i);

    }
    else {
      q_info->active_ids[q_info->active_num] = i;
      q_info->active_num++;
      q_info->send_vector[rm_id] = true;
    }
  }
  q_info->first_active_rm_id = mid_to_rmid(q_info->active_ids[0]);
  if (q_info->active_num > 0)
    q_info->last_active_rm_id = mid_to_rmid(q_info->active_ids[q_info->active_num - 1]);
  if (DEBUG_QUORUM) print_q_info(q_info);

  if (ENABLE_ASSERTIONS) {
    assert(q_info->missing_num <= REM_MACH_NUM);
    assert(q_info->active_num <= REM_MACH_NUM);
  }
}


static inline bool all_q_info_targets_are_reached(quorum_info_t *q_info,
                                                  uint16_t miss_i)
{
  for (int tar_i = 0; tar_i < q_info->num_of_credit_targets; ++tar_i) {
    if (q_info->credit_ptrs[tar_i][q_info->missing_ids[miss_i]] != q_info->targets[tar_i])
      return false;
  }
  return true;
}


// Bring back a machine
static inline void revive_machine(quorum_info_t *q_info,
                                  uint8_t revived_mach_id)
{

  uint8_t rm_id = mid_to_rmid(revived_mach_id);
  if (ENABLE_ASSERTIONS) {
    assert(revived_mach_id < MACHINE_NUM);
    assert(revived_mach_id != machine_id);
    assert(q_info->missing_num > 0);
    assert(!q_info->send_vector[rm_id]);
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
    if (DEBUG_QUORUM) my_printf(green, "After: Missing position %u, missing id %u, id to revive\n",
                                i, q_info->missing_ids[i], revived_mach_id);
}

// Update the links between the send Work Requests for broadcasts given the quorum information
static inline void update_bcast_wr_links(quorum_info_t *q_info, struct ibv_send_wr *wr, uint16_t t_id)
{
  if (MESSAGES_IN_BCAST != REM_MACH_NUM) {
    assert(ENABLE_MULTICAST);
    // bcast wr links need not be updated on multicasts
    return;
  }
  uint8_t prev_i = 0, avail_mach = 0;
  if (DEBUG_QUORUM) my_printf(green, "Worker %u fixing the links between the wrs \n", t_id);
  for (uint8_t i = 0; i < REM_MACH_NUM; i++) {
    wr[i].next = NULL;
    if (q_info->send_vector[i]) {
      if (avail_mach > 0) {
        for (uint16_t j = 0; j < MAX_BCAST_BATCH; j++) {
          if (DEBUG_QUORUM) my_printf(yellow, "Worker %u, wr %d points to %d\n", t_id, (REM_MACH_NUM * j) + prev_i, (REM_MACH_NUM * j) + i);
          wr[(REM_MACH_NUM * j) + prev_i].next = &wr[(REM_MACH_NUM * j) + i];
        }
      }
      avail_mach++;
      prev_i = i;
    }
  }
}

// Check credits, first see if there are credits from all active nodes, then if not and enough time has passed,
// transition to write_quorum broadcasts
static inline bool check_bcast_credits(uint16_t *credits,
                                       quorum_info_t *q_info,
                                       uint32_t *time_out_cnt,
                                       uint16_t *available_credits,
                                       uint16_t min_credits,
                                       uint16_t t_id)
{
  uint16_t i;
  // First check the active ids, to have a fast path when there are not enough credits
  for (i = 0; i < q_info->active_num; i++) {
    if (credits[q_info->active_ids[i]] < min_credits) {
      (*time_out_cnt)++;
      if (*time_out_cnt == CREDIT_TIMEOUT) {
        if (DEBUG_QUORUM)
          my_printf(red, "Worker %u timed_out on machine %u \n",
                    t_id, q_info->active_ids[i]);
        // assert(false);
        update_q_info(q_info, credits, min_credits, t_id);
        for (int wr_i = 0; wr_i < q_info->num_of_send_wrs; ++wr_i) {
          update_bcast_wr_links(q_info, q_info->send_wrs_ptrs[wr_i], t_id);
        }

        (*time_out_cnt) = 0;
      }
      return false;
    }
  }

  (*time_out_cnt) = 0;

  // then check the missing credits to see if we need to change the configuration
  if (q_info->missing_num > 0) {
    for (i = 0; i < q_info->missing_num; i++) {
      if (all_q_info_targets_are_reached(q_info, i)) {
        if (DEBUG_QUORUM)
          my_printf(red, "Worker %u revives machine %u \n", t_id, q_info->missing_ids[i]);
        revive_machine(q_info, q_info->missing_ids[i]);
        // printf("Wrkr %u, after reviving, active num %u, active_id %u, %u, %u, %u \n",
        //t_id, q_info->active_num, q_info->active_ids[0], q_info->active_ids[1],
        //       q_info->active_ids[2],q_info->active_ids[3]);

        for (int wr_i = 0; wr_i < q_info->num_of_send_wrs; ++wr_i) {
          update_bcast_wr_links(q_info, q_info->send_wrs_ptrs[wr_i], t_id);
        }
      }
    }
  }
  if (unlikely(q_info->active_num < REMOTE_QUORUM)) return false;
  //finally count credits
  uint16_t avail_cred = K_64_;
  //printf("avail cred %u\n", avail_cred);
  for (i = 0; i < q_info->active_num; i++) {
    if (ENABLE_ASSERTIONS) assert(q_info->active_ids[i] < MACHINE_NUM &&
                                  q_info->active_ids[i] != machine_id);
    if (credits[q_info->active_ids[i]] < avail_cred)
      avail_cred = credits[q_info->active_ids[i]];
  }
  *available_credits = avail_cred;
  return true;
}

static inline void decrease_credits(uint16_t credits[][MACHINE_NUM], quorum_info_t *q_info,
                                    uint16_t mes_sent, uint8_t vc)
{
  for (uint8_t i = 0; i < q_info->active_num; i++) {
    if (ENABLE_ASSERTIONS) {
      assert(credits[vc][q_info->active_ids[i]] >= mes_sent);
      assert(q_info->active_ids[i] != machine_id && q_info->active_ids[i] < MACHINE_NUM);
      //assert(q_info->active_num == REM_MACH_NUM); // debug no-failure case
    }
    credits[vc][q_info->active_ids[i]] -= mes_sent;
  }
}


static inline uint32_t get_last_message_of_bcast(uint16_t br_i,
                                                  quorum_info_t *q_info)
{
  if (ENABLE_ASSERTIONS) assert(br_i > 0);
  if (ENABLE_MULTICAST)
    return (uint32_t) ((br_i * MESSAGES_IN_BCAST) - 1);
  else return  (uint32_t)
      (((br_i - 1) * MESSAGES_IN_BCAST) + q_info->last_active_rm_id);
}

static inline uint32_t get_first_message_of_next_bcast(uint16_t br_i,
                                                       quorum_info_t *q_info)
{
  if (ENABLE_ASSERTIONS) assert(br_i > 0);
  if (ENABLE_MULTICAST)
    return (uint32_t) (br_i * MESSAGES_IN_BCAST) ;
  else return  (uint32_t)
      ((br_i * MESSAGES_IN_BCAST) + q_info->first_active_rm_id);
}


static inline void last_mes_of_bcast_point_to_frst_mes_of_next_bcast(uint16_t br_i,
                                                                     quorum_info_t *q_info,
                                                                     struct ibv_send_wr *send_wr)
{
  if (ENABLE_ASSERTIONS) assert(br_i > 0);
  send_wr[get_last_message_of_bcast(br_i, q_info)].next =
    &send_wr[get_first_message_of_next_bcast(br_i, q_info)];

}

static inline uint32_t get_first_mes_of_bcast(quorum_info_t *q_info)
{
  return (uint32_t) (ENABLE_MULTICAST ? 0 : q_info->first_active_rm_id);
}

static inline void flag_the_first_bcast_message_signaled(quorum_info_t *q_info,
                                                         struct ibv_send_wr *send_wr)
{
  send_wr[get_first_mes_of_bcast(q_info)].send_flags |= IBV_SEND_SIGNALED;
}

#endif //CONFIG_UTIL_H
