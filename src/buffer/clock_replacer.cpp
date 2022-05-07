#include "buffer/clock_replacer.h"

#define Pinned -1
CLOCKReplacer::CLOCKReplacer(size_t num_pages) {
  counter = 0;
  num_pages_ = num_pages;
  Rank = new int[num_pages];
  for(int i = 0; i < num_pages_; i++){
    Rank[i] = Pinned;
  }
}

CLOCKReplacer::~CLOCKReplacer(){
  delete [] Rank;
}

bool CLOCKReplacer::Victim(frame_id_t *frame_id) {
  int index = -1;
  for(int i = 0; i < num_pages_; i++){
    if(Rank[i] == Pinned) continue;
    else if(Rank[i] == 1) Rank[i] = 0;
    else index = i;
  }
  if(index == -1){
    for(int i = 0; i < num_pages_; i++){
      if(Rank[i] == Pinned) continue;
      else if(Rank[i] == 1) Rank[i] = 0;
      else index = i;
    }
    if(index == -1) return false;
    else {
      *frame_id = index;
      Pin(index);
      return true;
    }
  }
  else{
    *frame_id = index;
    Pin(index);
    return true;
  }
}

void CLOCKReplacer::Pin(frame_id_t frame_id) {
  if(frame_id < 0 || frame_id >= (frame_id_t)num_pages_) return;
  if(Rank[frame_id] != Pinned){
    counter--;
    Rank[frame_id] = Pinned;
  }
}

void CLOCKReplacer::Unpin(frame_id_t frame_id) {
  if(frame_id < 0 || frame_id >= (frame_id_t)num_pages_) return;
  if(Rank[frame_id] == Pinned){
    counter++;
  }
  Rank[frame_id] = 1;
}

size_t CLOCKReplacer::Size() {
  return counter;
}