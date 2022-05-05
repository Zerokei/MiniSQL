#include "page/bitmap_page.h"

template<size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  for(uint32_t i = 0; i < MAX_CHARS; ++i){
    for(uint8_t j = 0; j < 8; ++j){
      if(bytes[i] & (1<<j)) continue;
      else {
        bytes[i] |= (1<<j);
        page_offset = i*8+j;
        return true;
      }
    }
  }
  return false;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if(bytes[page_offset/8] & (1<<(page_offset & 0x7))){
    bytes[page_offset/8] ^= 1<<(page_offset & 0x7);
    return true;
  }
  return false;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  return (bytes[page_offset/8] & (1<<(page_offset & 0x7))) == 0;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return false;
}

template
class BitmapPage<64>;

template
class BitmapPage<128>;

template
class BitmapPage<256>;

template
class BitmapPage<512>;

template
class BitmapPage<1024>;

template
class BitmapPage<2048>;

template
class BitmapPage<4096>;