* req_cqe_error: (from /sys/class/infiniband/mlx5_0/ports/1/hw_counters)
On the send side, it can mean that the send buffer is not registered (and the work request is not "inlined")


* Ibv post send error 12
I believe it means there is no space in the send queue
It's typically a problem of selective signaling and send queue sizing


* Segfault inside ibv-post-send, in function: __memmove_avx_unaligned_erms()
Check the address of the sgl_list. Perhaps you forgot to have it point in the right place?

* Core files
They will not be generated when sanitizers are on