//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

#include "common/logger.h"
namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  const std::lock_guard<std::mutex> lock(latch_);
  frame_id_t free_frame_id = -1;
  if (free_list_.empty()) {
    auto evcit_ret = replacer_->Evict(&free_frame_id);
    if (!evcit_ret) return nullptr;
    LOG_DEBUG("free_list_ is empty, evcit:%d", free_frame_id);
  } else {
    free_frame_id = free_list_.front();
    free_list_.pop_front();
    LOG_DEBUG("free_list_ is not empty, size: %lu, free_frame_id:%d", free_list_.size(), free_frame_id);
  }
  BUSTUB_ASSERT(free_frame_id >= 0 && free_frame_id < (int)pool_size_, "invalid frame_id");

  Page *page = &pages_[free_frame_id];
  if (page->IsDirty()) {
    LOG_DEBUG("page is dirty, write first, frame_id:%d", free_frame_id);
    latch_.unlock();
    FlushPage(page->GetPageId());
    latch_.lock();
  }

  if (page->GetPageId() != INVALID_PAGE_ID) {
    page_table_.erase(page->GetPageId());
  }

  page->ResetMemory();
  auto allocated_page_id = AllocatePage();
  LOG_DEBUG("Allocated page_id:%d", allocated_page_id);
  page->page_id_ = allocated_page_id;
  page->pin_count_ = 1;  // todo ?
  page->is_dirty_ = false;
  page_table_[allocated_page_id] = free_frame_id;
  *page_id = allocated_page_id;
  replacer_->RecordAccess(free_frame_id);
  replacer_->SetEvictable(free_frame_id, false);
  return page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id) -> Page * {
  const std::lock_guard<std::mutex> lock(latch_);
  auto page = GetPage(page_id);
  if (page != nullptr) return page;
  // not in memory, read from disk
  frame_id_t free_frame_id = -1;

  if (free_list_.empty()) {
    auto evcit_ret = replacer_->Evict(&free_frame_id);
    if (!evcit_ret) return nullptr;
  } else {
    free_frame_id = free_list_.front();
    free_list_.pop_front();
  }

  page = &pages_[free_frame_id];
  if (page->IsDirty()) {
    latch_.unlock();
    FlushPage(page->GetPageId());  // todo dead lock? yes
    latch_.lock();
  }

  if (page->GetPageId() != INVALID_PAGE_ID) {
    page_table_.erase(page->GetPageId());
  }
  page->page_id_ = page_id;
  page->pin_count_ = 1;  // todo ?
  page->is_dirty_ = false;
  page_table_[page_id] = free_frame_id;
  page->ResetMemory();
  replacer_->RecordAccess(free_frame_id);
  replacer_->SetEvictable(free_frame_id, false);
  disk_manager_->ReadPage(page_id, page->GetData());
  return page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) -> bool {
  const std::lock_guard<std::mutex> lock(latch_);
  auto page = GetPage(page_id);
  if (page == nullptr) return false;
  if (page->GetPinCount() == 0) return false;
  page->pin_count_--;
  page->is_dirty_ = is_dirty;
  if (page->GetPinCount() == 0) {
    replacer_->SetEvictable(page_table_[page_id], true);
  }
  return true;
}

Page *BufferPoolManager::GetPage(page_id_t page_id) {
  if (page_id == INVALID_PAGE_ID) return nullptr;
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) return nullptr;
  return &pages_[it->second];
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  const std::lock_guard<std::mutex> lock(latch_);
  Page *page = GetPage(page_id);
  if (page == nullptr) return false;
  disk_manager_->WritePage(page_id, page->GetData());
  page->is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  for (auto &it : page_table_) {
    FlushPage(it.first);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  const std::lock_guard<std::mutex> lock(latch_);
  auto page = GetPage(page_id);
  if (page == nullptr) return true;
  if (page->GetPinCount() != 0) return false;
  if (page->IsDirty()) {
    latch_.unlock();
    FlushPage(page_id);
    latch_.lock();
  }
  auto frame_id = page_table_[page_id];
  page_table_.erase(page_id);
  replacer_->Remove(frame_id);
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  free_list_.push_back(frame_id);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto page = FetchPage(page_id);
  if (page != nullptr) {
    page->RLatch();
  }
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto page = FetchPage(page_id);
  if (page != nullptr) {
    page->WLatch();
  }
  return {this, nullptr};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }

}  // namespace bustub
