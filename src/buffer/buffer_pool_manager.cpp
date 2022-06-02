#include "buffer/buffer_pool_manager.h"
#include "glog/logging.h"
#include "page/bitmap_page.h"
#include "gtest/gtest.h"

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
        : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page: page_table_) {
    // printf("%d\n", page.first);
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  page_id_t new_page_id = page_id;
  frame_id_t new_frame_id = 0;

  if(page_table_.find(new_page_id) != page_table_.end()){// in the buffer pool manager
    pages_[page_table_[new_page_id]].pin_count_ = 1;
    return &pages_[page_table_[new_page_id]];
  }
  if(free_list_.empty()){
    if(replacer_->Victim(&new_frame_id) == false)
      return nullptr;
    else {
      page_id_t old_page_id = pages_[new_frame_id].GetPageId();
      EXPECT_TRUE(FlushPage(old_page_id));
      memset(pages_[new_frame_id].GetData(), 0, PAGE_SIZE);
      page_table_.erase(old_page_id);
      pages_[new_frame_id].page_id_ = INVALID_PAGE_ID;
      free_list_.emplace_back(new_frame_id);
    }
  }

  new_frame_id = free_list_.front();
  free_list_.pop_front();
  page_table_.insert({new_page_id, new_frame_id});
  
  pages_[new_frame_id].page_id_ = new_page_id;
  pages_[new_frame_id].pin_count_ = 1;
  pages_[new_frame_id].is_dirty_ = false;
  disk_manager_->ReadPage(new_page_id, pages_[new_frame_id].GetData());
  
  return &pages_[new_frame_id];
}

Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  frame_id_t new_frame_id = 0;
  if(free_list_.empty()){
    if(replacer_->Victim(&new_frame_id) == false)
      return nullptr;
    else {
      page_id_t old_page_id = pages_[new_frame_id].GetPageId();
      EXPECT_TRUE(FlushPage(old_page_id));
      memset(pages_[new_frame_id].GetData(), 0, PAGE_SIZE);
      pages_[new_frame_id].page_id_ = INVALID_PAGE_ID;
      page_table_.erase(old_page_id);
      free_list_.emplace_back(new_frame_id);
    }
  }
  new_frame_id = free_list_.front();
  free_list_.pop_front();
  page_id_t new_page_id = AllocatePage();
  page_table_.insert({new_page_id, new_frame_id});
  
  pages_[new_frame_id].page_id_ = new_page_id;
  // printf("Pin: %d->%d\n", new_frame_id, new_page_id);
  pages_[new_frame_id].pin_count_ = 1;
  pages_[new_frame_id].is_dirty_ = true;
  memset(pages_[new_frame_id].GetData(), 0, PAGE_SIZE);
  
  page_id = new_page_id;
  return &pages_[new_frame_id];
}

bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  if(page_table_.find(page_id) == page_table_.end()) {// not exist
    LOG(ERROR) << "DeletePage " << page_id << " does not exist!" << endl;
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  if(pages_[frame_id].GetPinCount() > 0) {
    LOG(ERROR) << "DeletePage " << page_id << " has been pinned!" << endl;
    return false;
  }
  else {
    DeallocatePage(page_id);
    page_table_.erase(page_id);
    pages_[frame_id].page_id_ = INVALID_PAGE_ID;
    pages_[frame_id].is_dirty_ = false;
    pages_[frame_id].pin_count_ = 0;
    free_list_.emplace_back(frame_id);
    return true;
  }
}

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  if(page_table_.find(page_id) == page_table_.end()) {// not exist
    LOG(ERROR) << "UnpinPage " << page_id << " does not exist!" << endl;
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];  
  replacer_->Unpin(frame_id);
  pages_[frame_id].pin_count_--;
  pages_[frame_id].is_dirty_ |= is_dirty;
  return true;
}

bool BufferPoolManager::FlushPage(page_id_t page_id) {
  if(page_table_.find(page_id) == page_table_.end()) {// not exist
    LOG(ERROR) << "FlsuhPage " << page_id << " does not exist!" << endl;
    return false;
  }

  frame_id_t frame_id = page_table_[page_id];  
  // whatever it is pinned
  disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 0;
  return true;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}