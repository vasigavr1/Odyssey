* req_cqe_error: (from /sys/class/infiniband/mlx5_0/ports/1/hw_counters)
On the send side, it can mean that the send buffer is not registered (and the work request is not "inlined").
Usually, the message appears to be sent with no blocking or error on the send side. 
But because the buffer is not registered it is silently never sent.
Tip: Decrease coalescing to fall below the inline limit, to confirm the issue.


* Ibv post send error 12
I believe it means there is no space in the send queue
It's typically a problem of selective signaling and send queue sizing


* Segfault inside ibv-post-send, in function: __memmove_avx_unaligned_erms()
Check the address of the sgl_list. Perhaps you forgot to have it point in the right place?

* Core files
They will not be generated when sanitizers are on

* No ib devices found
If ibstat and ibstatus show that the state is ACTIVE but ibv_devinfo returns that 
it found no ib devices, then you need to execute ibdev2netdev

* MLNX bugs
Take a look at the ofed cheat sheet:
https://gist.github.com/githubfoam/da75951b97e9aec21dcebadf68a6a360