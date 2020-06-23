//
// Created by vasilis on 23/06/2020.
//

#ifndef KITE_GENERIC_FUNC_H
#define KITE_GENERIC_FUNC_H

#include "top.h"
#include <getopt.h>

void handle_program_inputs(int argc, char *argv[])
{
  int c;
  char *tmp_ip;
  static struct option opts[] = {
    { .name = "machine-id",			.has_arg = 1, .val = 'm' },
    { .name = "is-roce",			.has_arg = 1, .val = 'r' },
    { .name = "all-ips",       .has_arg = 1, .val ='a'},
    { .name = "device_name",			.has_arg = 1, .val = 'd'},
    { 0 }
  };

  /* Parse and check arguments */
  while(true) {
    c = getopt_long(argc, argv, "M:t:b:N:n:c:u:m:p:r:i:l:x", opts, NULL);
    if(c == -1) {
      break;
    }
    switch (c) {
      case 'm':
        machine_id = atoi(optarg);
        break;
      case 'r':
        is_roce = atoi(optarg);
        break;
      case 'a':
        tmp_ip = optarg;
        break;
      case 'd':
        dev_name = optarg;
        break;
      default:
        printf("Invalid argument %d\n", c);
        assert(false);
    }
  }
  if (machine_id == -1) assert(false);
  char* chars_array = strtok(tmp_ip, ",");
  remote_ips = (char **) (malloc(REM_MACH_NUM * sizeof(char *)));
  int rm_id = 0;
  for ( int m_id = 0; m_id < MACHINE_NUM; m_id++) {
    assert(chars_array != NULL); // not enough IPs were passed
    printf("ip %s \n", chars_array);
    if (m_id ==  machine_id) {
      local_ip = (char *) (malloc(16));
      memcpy(local_ip, chars_array, strlen(chars_array) + 1);
      printf("local_ip = %s  \n", local_ip);
    }
    else {
      remote_ips[rm_id] = (char *) (malloc(16));
      memcpy(remote_ips[rm_id], chars_array, strlen(chars_array) + 1);
      printf("remote_ip[%d] = %s  \n", rm_id, remote_ips[rm_id]);
      rm_id++;
    }

    //else remote_ip_vector.push_back(chars_array);
    chars_array = strtok(NULL, ",");
  }
}

#endif //KITE_GENERIC_FUNC_H
