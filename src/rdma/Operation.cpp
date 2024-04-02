#include "Rdma.h"
#include <atomic>
#include <cstdint>
#include <cstring>
#include <iostream>

int pollWithCQ(ibv_cq *cq, int pollNumber, struct ibv_wc *wc) {
  
  return 0;
}

int pollOnce(ibv_cq *cq, int pollNumber, struct ibv_wc *wc) {
  return 0;
}

static inline void fillSgeWr(ibv_sge &sg, ibv_send_wr &wr, uint64_t source,
                             uint64_t size, uint32_t lkey) {
  memset(&sg, 0, sizeof(sg));
  sg.addr = (uintptr_t)source;
  sg.length = size;
  sg.lkey = lkey;

  memset(&wr, 0, sizeof(wr));
  wr.wr_id = 0;
  wr.sg_list = &sg;
  wr.num_sge = 1;
}

static inline void fillSgeWr(ibv_sge &sg, ibv_recv_wr &wr, uint64_t source,
                             uint64_t size, uint32_t lkey) {
  memset(&sg, 0, sizeof(sg));
  sg.addr = (uintptr_t)source;
  sg.length = size;
  sg.lkey = lkey;

  memset(&wr, 0, sizeof(wr));
  wr.wr_id = 0;
  wr.sg_list = &sg;
  wr.num_sge = 1;
}

static inline void fillSgeWr(ibv_sge &sg, ibv_exp_send_wr &wr, uint64_t source,
                             uint64_t size, uint32_t lkey) {
  memset(&sg, 0, sizeof(sg));
  sg.addr = (uintptr_t)source;
  sg.length = size;
  sg.lkey = lkey;

  memset(&wr, 0, sizeof(wr));
  wr.wr_id = 0;
  wr.sg_list = &sg;
  wr.num_sge = 1;
}

// for UD and DC
bool rdmaSend(ibv_qp *qp, uint64_t source, uint64_t size, uint32_t lkey,
              ibv_ah *ah, uint32_t remoteQPN /* remote dct_number */,
              bool isSignaled) {

  struct ibv_sge sg;
  struct ibv_send_wr wr;
  struct ibv_send_wr *wrBad;

  fillSgeWr(sg, wr, source, size, lkey);

  wr.opcode = IBV_WR_SEND;

  wr.wr.ud.ah = ah;
  wr.wr.ud.remote_qpn = remoteQPN;
  wr.wr.ud.remote_qkey = UD_PKEY;

  if (isSignaled)
    wr.send_flags = IBV_SEND_SIGNALED;
  if (ibv_post_send(qp, &wr, &wrBad)) {
    Debug::notifyError("Send with RDMA_SEND failed.");
    return false;
  }
  return true;
}

// for RC & UC
bool rdmaSend(ibv_qp *qp, uint64_t source, uint64_t size, uint32_t lkey,
              int32_t imm) {

  struct ibv_sge sg;
  struct ibv_send_wr wr;
  struct ibv_send_wr *wrBad;

  fillSgeWr(sg, wr, source, size, lkey);

  if (imm != -1) {
    wr.imm_data = imm;
    wr.opcode = IBV_WR_SEND_WITH_IMM;
  } else {
    wr.opcode = IBV_WR_SEND;
  }

  wr.send_flags = IBV_SEND_SIGNALED;

  if (ibv_post_send(qp, &wr, &wrBad)) {
    Debug::notifyError("Send with RDMA_SEND failed.");
    return false;
  }
  return true;
}

bool rdmaReceive(ibv_qp *qp, uint64_t source, uint64_t size, uint32_t lkey,
                 uint64_t wr_id) {
  struct ibv_sge sg;
  struct ibv_recv_wr wr;
  struct ibv_recv_wr *wrBad;

  fillSgeWr(sg, wr, source, size, lkey);

  wr.wr_id = wr_id;

  if (ibv_post_recv(qp, &wr, &wrBad)) {
    Debug::notifyError("Receive with RDMA_RECV failed.");
    return false;
  }
  return true;
}

bool rdmaReceive(ibv_srq *srq, uint64_t source, uint64_t size, uint32_t lkey) {

  struct ibv_sge sg;
  struct ibv_recv_wr wr;
  struct ibv_recv_wr *wrBad;

  fillSgeWr(sg, wr, source, size, lkey);

  if (ibv_post_srq_recv(srq, &wr, &wrBad)) {
    Debug::notifyError("Receive with RDMA_RECV failed.");
    return false;
  }
  return true;
}



// for RC & UC
bool rdmaRead(ibv_qp *qp, uint64_t source, uint64_t dest, uint64_t size,
              uint32_t lkey, uint32_t remoteRKey, bool signal, uint64_t wrID) {
  // 首先，将整数地址转换为指向void的指针
    void* src_ptr = reinterpret_cast<void*>(source);
    void* dest_ptr = reinterpret_cast<void*>(dest);

    // 检查指针是否为nullptr，并且size不为0
    if (src_ptr == nullptr || dest_ptr == nullptr || size == 0) {
        return false;
    }

    // 使用memcpy来复制数据
    std::memcpy(src_ptr, dest_ptr, 8);
    return true;
}


// for RC & UC
bool rdmaWrite(ibv_qp *qp, uint64_t source, uint64_t dest, uint64_t size,
               uint32_t lkey, uint32_t remoteRKey, int32_t imm, bool isSignaled,
               uint64_t wrID) {

    // 将整数地址转换为指向void的指针
    void* src_ptr = reinterpret_cast<void*>(source);
    void* dest_ptr = reinterpret_cast<void*>(dest);

    // 检查指针是否为nullptr
    if (src_ptr == nullptr || dest_ptr == nullptr) {
        return false;
    }

    // 使用memcpy来复制数据
    std::memcpy(dest_ptr, src_ptr, 8);
    return true;

}

// RC & UC
bool rdmaFetchAndAdd(ibv_qp *qp, uint64_t source, uint64_t dest, uint64_t add,
                     uint32_t lkey, uint32_t remoteRKey) {
  // 使用 std::atomic_ref<T> 需要数据满足 TriviallyCopyable 概念
    static_assert(std::is_trivially_copyable<uint64_t>::value, "Data must be trivially copyable");

    try {
        // 创建一个原子引用,指向 dest 地址的数据
        std::atomic_ref<uint64_t> atomic(*(reinterpret_cast<uint64_t*>(dest)));

        // 对原子引用执行 fetch_add 操作
        atomic.fetch_add(add);

        // 如果操作成功,返回 true
        return true;
    } catch (...) {
        // 如果操作失败(例如,由于 dest 是一个无效的地址),返回 false
        return false;
    }

}

bool rdmaFetchAndAddBoundary(ibv_qp *qp, uint64_t source, uint64_t dest,
                             uint64_t add, uint32_t lkey, uint32_t remoteRKey,
                             uint64_t boundary, bool singal, uint64_t wr_id) {
   // 将 uint64_t 类型的 dest 转换为指向原子变量的指针
    std::atomic<uint64_t>* atomic_dest_ptr = reinterpret_cast<std::atomic<uint64_t>*>(dest);

    // 我们假设 boundary 是一个有效的位数限制
    uint64_t mask = (boundary >= 64) ? ~uint64_t(0) : (1ull << boundary) - 1;

    // 读取当前值，进行操作，然后尝试原子地更新它
    uint64_t old_value = atomic_dest_ptr->load(std::memory_order_relaxed);
    uint64_t new_value;
    bool success;
    do {
        new_value = (old_value + add) & mask;
        // compare_exchange_weak 在多线程环境下可能会失败，因此使用循环
        success = atomic_dest_ptr->compare_exchange_weak(old_value, new_value,
                                                         std::memory_order_release,
                                                         std::memory_order_relaxed);
    } while (!success);

    return new_value >= old_value;

}


// for RC & UC
bool rdmaCompareAndSwap(ibv_qp *qp, uint64_t source, uint64_t dest,
                        uint64_t compare, uint64_t swap, uint32_t lkey,
                        uint32_t remoteRKey, bool signal, uint64_t wrID) {
   std::atomic_ref<uint64_t> atomic(*(reinterpret_cast<uint64_t*>(dest)));
            return atomic.compare_exchange_strong(compare, swap);

}

bool rdmaCompareAndSwapMask(ibv_qp *qp, uint64_t source, uint64_t dest,
                            uint64_t compare, uint64_t swap, uint32_t lkey,
                            uint32_t remoteRKey, uint64_t mask, bool singal, uint64_t wrID) {
  auto* atomic_dest = reinterpret_cast<std::atomic<uint64_t>*>(dest);
    uint64_t expected = atomic_dest->load(std::memory_order_relaxed);
    uint64_t desired;
    do {
        desired = (expected & ~mask) | (swap & mask);
        if (atomic_dest->compare_exchange_weak(expected, desired,
                                               std::memory_order_release,
                                               std::memory_order_relaxed)) {
            return true;
        }
    } while ((expected & mask) == (compare & mask));
    return false;
  
}


bool rdmaReadBatch(ibv_qp *qp, RdmaOpRegion *ror, int k, bool isSignaled,
                   uint64_t wrID) {
  // For range_query corotine safety
  // thread_local struct ibv_sge coro_sg[MAX_CORO_NUM][kReadOroMax];
  // thread_local struct ibv_send_wr coro_wr[MAX_CORO_NUM][kReadOroMax];

  // auto& sg = coro_sg[wrID];
  // auto& wr = coro_wr[wrID];

   for (int i = 0; i < k; ++i) {
   rdmaRead(qp, ror[i].source, ror[i].dest, ror[i].size,
              ror[i].lkey, ror[i].remoteRKey, isSignaled, wrID);
   }
  return true;

}


bool rdmaWriteBatch(ibv_qp *qp, RdmaOpRegion *ror, int k, bool isSignaled,
                    uint64_t wrID) {

  for (int i = 0; i < k; ++i) {
rdmaWrite(qp, ror[i].source, ror[i].dest, ror[i].size,
              ror[i].lkey, ror[i].remoteRKey, isSignaled, wrID);
      }
  return true;

}

bool rdmaCasRead(ibv_qp *qp, const RdmaOpRegion &cas_ror,
                 const RdmaOpRegion &read_ror, uint64_t compare, uint64_t swap,
                 bool isSignaled, uint64_t wrID) {

   rdmaCompareAndSwap(qp, cas_ror.source, cas_ror.dest,
                        compare, swap, cas_ror.lkey,
                        cas_ror.remoteRKey, wrID);

   rdmaRead(qp, read_ror.source, read_ror.dest, read_ror.size,
              read_ror.lkey, read_ror.remoteRKey, isSignaled, wrID);
   return true;

}

bool rdmaReadCas(ibv_qp *qp, const RdmaOpRegion &read_ror,
                 const RdmaOpRegion &cas_ror, uint64_t compare, uint64_t swap,
                 bool isSignaled, uint64_t wrID) {

  rdmaRead(qp, read_ror.source, cas_ror.dest, read_ror.size,
              read_ror.lkey, cas_ror.remoteRKey, isSignaled, wrID);
   rdmaCompareAndSwap(qp, cas_ror.source, read_ror.dest,
                        compare, swap, cas_ror.lkey,
                        read_ror.remoteRKey, wrID);
  return true;

}

bool rdmaCasWrite(ibv_qp *qp, const RdmaOpRegion &cas_ror,
                  const RdmaOpRegion &write_ror, uint64_t compare, uint64_t swap,
                  bool isSignaled, uint64_t wrID) {

 rdmaCompareAndSwap(qp, cas_ror.source, cas_ror.dest,
                        compare, swap, cas_ror.lkey,
                        cas_ror.remoteRKey, wrID);
 
  rdmaWrite(qp, write_ror.source, write_ror.dest, write_ror.size,
               write_ror.lkey, write_ror.remoteRKey, -1, isSignaled,
               wrID);
  return true;

}

bool rdmaWriteFaa(ibv_qp *qp, const RdmaOpRegion &write_ror,
                  const RdmaOpRegion &faa_ror, uint64_t add_val,
                  bool isSignaled, uint64_t wrID) {

  rdmaWrite(qp, write_ror.source, write_ror.dest, write_ror.size,
               write_ror.lkey, write_ror.remoteRKey, -1, isSignaled,
               wrID);

 
 rdmaFetchAndAdd(qp, faa_ror.source, faa_ror.dest, add_val,
                     faa_ror.lkey, faa_ror.remoteRKey);
   return true;
}

bool rdmaWriteCas(ibv_qp *qp, const RdmaOpRegion &write_ror,
                  const RdmaOpRegion &cas_ror, uint64_t compare, uint64_t swap,
                  bool isSignaled, uint64_t wrID) {

  rdmaWrite(qp, write_ror.source, write_ror.dest, write_ror.size,
               write_ror.lkey, write_ror.remoteRKey, -1, isSignaled,
               wrID);

    rdmaCompareAndSwap(qp, cas_ror.source, cas_ror.dest,
                        compare, swap, cas_ror.lkey,
                        cas_ror.remoteRKey, wrID);

    return true;

  
}

bool rdmaWriteCasMask(ibv_qp *qp, const RdmaOpRegion &write_ror,
                  const RdmaOpRegion &cas_ror, uint64_t compare, uint64_t swap, uint64_t mask,
                  bool isSignaled, uint64_t wrID) {

  rdmaWrite(qp, write_ror.source, write_ror.dest, write_ror.size,
               write_ror.lkey, write_ror.remoteRKey, -1, isSignaled,
               wrID);
   rdmaCompareAndSwapMask(qp, cas_ror.source, cas_ror.dest,
                            compare, swap, cas_ror.lkey,
                            cas_ror.remoteRKey, mask, isSignaled, wrID);
    return true;
}

bool rdmaTwoCasMask(ibv_qp *qp, const RdmaOpRegion &cas_ror_1, uint64_t compare_1, uint64_t swap_1, uint64_t mask_1,
                    const RdmaOpRegion &cas_ror_2, uint64_t compare_2, uint64_t swap_2, uint64_t mask_2,
                    bool isSignaled, uint64_t wrID) {

  rdmaCompareAndSwapMask(qp, cas_ror_1.source, cas_ror_1.dest,
                            compare_1, swap_1, cas_ror_1.lkey,
                            cas_ror_1.remoteRKey, mask_1, isSignaled, wrID);
  rdmaCompareAndSwapMask(qp, cas_ror_2.source, cas_ror_2.dest,
                            compare_2, swap_2, cas_ror_2.lkey,
                            cas_ror_2.remoteRKey, mask_2, isSignaled, wrID);
   return true;
}
