#include "buffer/lru_replacer.h"

#define Pinned -1
LRUReplacer::LRUReplacer(size_t num_pages) {
  counter = 0;
  num_pages_ = num_pages;
  Rank = new int[num_pages];
  for(int i = 0; i < num_pages_; i++){
    Rank[i] = Pinned;
  }
}

LRUReplacer::~LRUReplacer(){
  delete [] Rank;
}

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  int index = -1, mx = 0;
  for(int i = 0; i < num_pages_; i++){
    if(Rank[i] == Pinned || Rank[i] <= mx) continue;
    index = i;
    mx = Rank[i];
  }
  if(index == -1) return false;
  else{
    *frame_id = index;
    Pin(index);
    return true;
  }
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  if(frame_id < 0 || frame_id >= (frame_id_t)num_pages_) return;
  if(Rank[frame_id] != Pinned){
    counter--;
    Rank[frame_id] = Pinned;
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  if(frame_id < 0 || frame_id >= (frame_id_t)num_pages_) return;
  if(Rank[frame_id] == Pinned){
    counter++;
    Rank[frame_id] = 0;
  }
  for(int i = 0; i < num_pages_; i++)
    if(Rank[i] != Pinned) Rank[i]++;
}

size_t LRUReplacer::Size() {
  return counter;
}