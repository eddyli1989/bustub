#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
    page_ = that.page_;
    bpm_ = that.bpm_;
    is_dirty_ = that.is_dirty_;
    
    that.page_ = nullptr;
    that.bpm_ = nullptr;
    that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
    bpm_ = nullptr;
    page_ = nullptr;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & { return *this; }

BasicPageGuard::~BasicPageGuard() {
    Drop();
};

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept = default;

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & { return *this; }

void ReadPageGuard::Drop() {
    guard_.page_->RUnlatch();
    guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() {
    Drop();
}

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept = default;

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & { return *this; }

void WritePageGuard::Drop() {
    guard_.page_->WUnlatch();
    guard_.Drop();
}

WritePageGuard::~WritePageGuard() {
    Drop();
}

}  // namespace bustub
