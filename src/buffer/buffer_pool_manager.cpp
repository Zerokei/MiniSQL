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
  frame_id_t new_frame_id = page_id, new_page_id = 0;
  if(free_list_.empty()){
    if(replacer_->Victim(&new_page_id) == false)
      return nullptr;
    else EXPECT_TRUE(FlushPage(new_page_id));
  }
  new_page_id = free_list_.front();
  free_list_.pop_front();
  page_table_[new_page_id] = new_frame_id;
  
  pages_[new_page_id].page_id_ = new_frame_id;
  pages_[new_page_id].pin_count_ = 1;
  pages_[new_page_id].is_dirty_ = false;
  disk_manager_->ReadPage(new_frame_id, pages_[new_page_id].GetData());
  
  return &pages_[new_page_id];
}

Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  frame_id_t new_page_id = 0;
  if(free_list_.empty()){
    if(replacer_->Victim(&new_page_id) == false)
      return nullptr;
    else {
      EXPECT_TRUE(FlushPage(new_page_id));
    }
  }
  new_page_id = free_list_.front();
  free_list_.pop_front();
  frame_id_t new_frame_id = AllocatePage();
  page_table_[new_page_id] = new_frame_id;
  
  pages_[new_page_id].page_id_ = new_frame_id;
  pages_[new_page_id].pin_count_ = 1;
  pages_[new_page_id].is_dirty_ = true;
  memset(pages_[new_page_id].GetData(), 0, PAGE_SIZE);
  
  page_id = new_frame_id;
  return &pages_[new_page_id];
}

bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  for(int i = 0; i < (int)pool_size_; i++){
    if(pages_[i].GetPageId() == page_id) {
      if(pages_[i].GetPinCount() > 0) return false;
      else {
        DeallocatePage(page_table_[i]);
        page_table_.erase(i);
        pages_[i].page_id_ = INVALID_PAGE_ID;
        memset(pages_[i].GetData(), 0, PAGE_SIZE);
        free_list_.emplace_back(i);
        return true;
      }
    }
  }
  // not exist
  return true;
}

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  for(int i = 0; i < (int)pool_size_; i++){
    if(page_table_.find(i) != page_table_.end() && page_table_[i] == page_id){
      replacer_->Unpin(page_table_[i]);
      pages_[i].pin_count_--;
      pages_[i].is_dirty_ |= is_dirty;
      return true;
    }
  }
  return false;
}

bool BufferPoolManager::FlushPage(page_id_t page_id) {
  if(pages_[page_id].GetPinCount() > 0) return false;
  else {
    disk_manager_->WritePage(page_table_[page_id], pages_[page_id].GetData());
    memset(pages_[page_id].GetData(), 0, PAGE_SIZE);
    pages_[page_id].is_dirty_ = false;
    free_list_.emplace_back(page_id);
    return true;
  }
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