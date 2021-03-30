#ifndef HERMES_HERMES_ASYNC_H
#define HERMES_HERMES_ASYNC_H

#include "config.h"
#include "spacetime.h"

#define MAX_READ_BUFFER_SIZE 512

#define ENABLE_RD_BUF_COALESCING

#define EMPTY_ASYNC_TS 0

static_assert(MAX_MACHINE_NUM > 2, "");

typedef uint64_t async_ts;

typedef struct {
    async_ts ts;
    uint16_t cnt;
    spacetime_op_t* ptr;
} read_buf_slot_ptr;

typedef struct {
    uint16_t curr_len;
    read_buf_slot_ptr     ptrs[MAX_READ_BUFFER_SIZE];
    spacetime_op_t read_memory[MAX_READ_BUFFER_SIZE];
} read_buf_t;

static void read_buf_init(read_buf_t* rb){
    rb->curr_len = 0;
    for(int i = 0; i < MAX_READ_BUFFER_SIZE; i++){
        rb->ptrs[i].ts = 0;
        rb->ptrs[i].cnt = 0;
        rb->ptrs[i].ptr = &rb->read_memory[i];
    }
}

typedef struct {
    uint16_t worker_lid;
    struct timespec* stopwatch_for_req_latency;
    volatile long long* worker_completed_ops;
    async_ts curr_ts;
    async_ts majority_ts;
    uint8_t  has_given_ts;
    uint8_t  has_issued_upd;
    async_ts ts_array[MAX_MACHINE_NUM];
    async_ts op_async_ts[MAX_BATCH_KVS_OPS_SIZE];
    read_buf_t rb;
} async_ctx;

static inline void complete_reads_from_read_buf(async_ctx *a_ctx);

static void async_init(async_ctx* a_ctx,
                       volatile long long* worker_completed_ops,
                       uint16_t worker_lid,
                       struct timespec* stopwatch_for_req_latency)
{
    a_ctx->worker_lid = worker_lid;
    a_ctx->stopwatch_for_req_latency = stopwatch_for_req_latency;
    a_ctx->worker_completed_ops = worker_completed_ops;
    a_ctx->curr_ts     = EMPTY_ASYNC_TS;
    a_ctx->majority_ts = EMPTY_ASYNC_TS;
    a_ctx->has_issued_upd = 0;
    for(int i = 0; i < MAX_MACHINE_NUM; ++i){
        a_ctx->ts_array[i]    = EMPTY_ASYNC_TS;
    }
    for(int i = 0; i < MAX_BATCH_KVS_OPS_SIZE; ++i){
        a_ctx->op_async_ts[i] = EMPTY_ASYNC_TS;
    }

    read_buf_init(&a_ctx->rb);
}

static inline void async_start_cycle(async_ctx* a_ctx)
{
    a_ctx->curr_ts++;
    a_ctx->has_given_ts = 0;
    a_ctx->has_issued_upd = 0;
}

static inline void async_end_cycle(async_ctx* a_ctx)
{
    // WARNING we need to ensure there is at least one write per cycle that contains a read
//    assert(!a_ctx->has_given_ts || a_ctx->has_issued_upd);
}

static inline void async_reset_op_ts(async_ctx* a_ctx, uint16_t op_idx)
{
    a_ctx->op_async_ts[op_idx] = EMPTY_ASYNC_TS;
}

static inline void async_set_op_ts(async_ctx* a_ctx, uint16_t op_idx, uint8_t is_upd)
{
    a_ctx->op_async_ts[op_idx] = a_ctx->curr_ts;
    a_ctx->has_given_ts = 1;
    a_ctx->has_issued_upd |= is_upd;
}

static inline void async_set_ops_ts(async_ctx* a_ctx, spacetime_op_t *ops){
    async_start_cycle(a_ctx);

    // completed read or pending update
    for(int i = 0; i < max_batch_size; i++) {
        uint8_t just_completed_read =
                ops[i].op_meta.state == ST_GET_COMPLETE &&
                a_ctx->op_async_ts[i] == EMPTY_ASYNC_TS;
        uint8_t unsent_upd =
                ops[i].op_meta.state == ST_PUT_SUCCESS ||
                ops[i].op_meta.state == ST_RMW_SUCCESS;

        if(just_completed_read || unsent_upd){
            async_set_op_ts(a_ctx, i, unsent_upd);
        }
    }

    async_end_cycle(a_ctx);
}

static inline void async_complete_read(async_ctx* a_ctx, spacetime_op_t *op, uint16_t op_id)
{
    if(op->op_meta.state == ST_GET_COMPLETE &&
       a_ctx->op_async_ts[op_id] <= a_ctx->majority_ts)
    {
        op->op_meta.state = ST_GET_ASYNC_COMPLETE;
    }
}

static inline void async_complete_reads(async_ctx* a_ctx, spacetime_op_t *ops)
{
    for(int i = 0; i < max_batch_size; i++) {
        async_complete_read(a_ctx, &ops[i], i);
    }
}

static inline void async_set_ack_ts(async_ctx* a_ctx, spacetime_op_t *ops,
                                    int op_idx, uint8_t node_id,
                                    const uint8_t set_read_completion)
{
    async_ts ts = a_ctx->op_async_ts[op_idx];

    if(a_ctx->ts_array[node_id] < ts){
        a_ctx->ts_array[node_id] = ts;
    }

    if(a_ctx->majority_ts >= ts) return;

    // calculate majority ts
    uint8_t majority = machine_num / 2; // + 1 (the local replica)
    for(int i = 0; i < machine_num; ++i){
        if(a_ctx->ts_array[i] >= ts){
            majority--;
            if(majority == 0){
                a_ctx->majority_ts = ts;
                if(set_read_completion){ // set read completion if flagged
                    async_complete_reads(a_ctx, ops);
                }
#ifdef ENABLE_READ_BUFFER
                complete_reads_from_read_buf(a_ctx);
#endif
                return;
            }
        }
    }
}


static inline void complete_reads_from_read_buf(async_ctx *a_ctx)
{
    read_buf_t *rb = &a_ctx->rb;
    uint16_t total_completed = 0;
    async_ts mj_ts = a_ctx->majority_ts;
    read_buf_slot_ptr *ptrs = rb->ptrs;

    for(int i = 0; i < rb->curr_len; i++){
        if(ptrs[i].ts <= mj_ts){
            // account for completed req
            total_completed += ptrs[i].cnt;

            // remove from buffer
            if(i == rb->curr_len - 1){
                rb->curr_len--;
                break; // break if last item
            }
            // remove by swapping
            read_buf_slot_ptr last_ptr = ptrs[rb->curr_len - 1];
            ptrs[rb->curr_len - 1].ptr = ptrs[i].ptr;
            ptrs[i] = last_ptr;
            rb->curr_len--;
            i--; //recheck same idx since we swapped with last ptr
        }
    }

    *a_ctx->worker_completed_ops =
            *a_ctx->worker_completed_ops + total_completed;
}

static inline void try_add_op_to_read_buf(async_ctx *a_ctx,
                                          spacetime_op_t* src,
                                          uint16_t src_op_idx)
{
    // return if not appropriate state(op) or if buf is full
    if(src->op_meta.state != ST_GET_COMPLETE) return;
    assert(src->op_meta.opcode == ST_OP_GET);

    uint16_t curr_len = a_ctx->rb.curr_len;
    async_ts op_ts = a_ctx->op_async_ts[src_op_idx];

#ifdef ENABLE_RD_BUF_COALESCING
    for(int i = 0; i < curr_len; ++i){
        if(a_ctx->rb.ptrs[i].ts == op_ts){
            a_ctx->rb.ptrs[i].cnt++;
            src->op_meta.state = ST_MISS; // Making it a MISS will be resetted afterwards
            return;
        }
    }
#endif

    if(curr_len == MAX_READ_BUFFER_SIZE - 1) return;

    read_buf_slot_ptr *rb_slot = &a_ctx->rb.ptrs[curr_len];
    *rb_slot->ptr = *src;
    rb_slot->cnt = 1;
    rb_slot->ts = op_ts;
    a_ctx->rb.curr_len++;

    src->op_meta.state = ST_MISS; // Making it a MISS will be resetted afterwards
}

#endif //HERMES_HERMES_ASYNC_H
