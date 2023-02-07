//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKNode::LRUKNode(size_t k, frame_id_t fid) : k_(k), fid_(fid) {}

void LRUKNode::SetEvictable(bool evictable) { is_evictable_ = evictable; }

auto LRUKNode::IsEvictable() -> bool { return is_evictable_; }

auto LRUKNode::GetFid() -> frame_id_t { return fid_; }

auto LRUKNode::GetBackwardKDist(size_t current_timestamp) -> size_t {
  if (HasInfBackwardKDist()) return -1;
  return current_timestamp - (history_.back());
}

auto LRUKNode::HasInfBackwardKDist() -> bool { return history_.size() < k_; }

auto LRUKNode::GetEarliestTimestamp() -> size_t {
  if (history_.size() == 0) return 0;
  return *history_.begin();
}

void LRUKNode::InsertHistoryTimestamp(size_t current_timestamp) {
  if (history_.size() >= k_) {
    history_.pop_front();  //  todo? update the last or pop front??
  }
  history_.push_back(current_timestamp);
}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  size_t max_dist = 0;
  bool has_inf = false;
  size_t inf_max = 0;
  frame_id_t evict_frame_id = -1;

  const std::lock_guard<std::mutex> lock(latch_);
  for (auto &node : node_store_) {
    if (!node.second.IsEvictable()) continue;
    if (node.second.HasInfBackwardKDist()) {
      has_inf = true;
      auto dist = current_timestamp_ - node.second.GetEarliestTimestamp();  // todo??
      if (dist >= inf_max) {
        inf_max = dist;
        evict_frame_id = node.first;
      }
      continue;
    }
    if (has_inf) continue;
    auto dist = node.second.GetBackwardKDist(current_timestamp_);
    if (dist >= max_dist) {
      max_dist = dist;
      evict_frame_id = node.first;
    }
  }
  if (evict_frame_id != -1) {
    *frame_id = evict_frame_id;
    latch_.unlock();
    Remove(evict_frame_id);
    latch_.lock();
    return true;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) < replacer_size_, "invalid frame_id");
  const std::lock_guard<std::mutex> lock(latch_);
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    if (node_store_.size() >= replacer_size_) {
      frame_id_t evict_frame;
      latch_.unlock();
      auto evict = Evict(&evict_frame);
      latch_.lock();
      if (!evict) return;
    }
    LRUKNode node(k_, frame_id);
    node.InsertHistoryTimestamp(current_timestamp_++);
    node_store_.insert(std::make_pair(frame_id, node));
    return;
  }
  it->second.InsertHistoryTimestamp(current_timestamp_++);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  const std::lock_guard<std::mutex> lock(latch_);
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    return;
  }
  if (!it->second.IsEvictable() && set_evictable) curr_size_++;
  if (it->second.IsEvictable() && !set_evictable) curr_size_--;
  it->second.SetEvictable(set_evictable);
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) < replacer_size_, "invalid frame_id");
  const std::lock_guard<std::mutex> lock(latch_);
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    return;
  }
  BUSTUB_ASSERT(it->second.IsEvictable(), "Not Evictable when remove");
  node_store_.erase(it);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
