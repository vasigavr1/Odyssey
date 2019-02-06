//
// Created by vasilis on 05/02/19.
//

#include "util.h"
#include "inline_util.h"

void *client(void *arg) {
  struct thread_params params = *(struct thread_params *) arg;
  uint16_t t_id = (uint16_t) params.id;
  uint16_t gid = (uint16_t)((machine_id * WORKERS_PER_MACHINE) + t_id);
  green_printf("Client %u reached loop \n");
  while (true) {

  }
}