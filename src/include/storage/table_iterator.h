#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "record/row.h"
#include "transaction/transaction.h"
#include "utils/mem_heap.h"


class TableHeap;

class TableIterator {

public:

  explicit TableIterator(int32_t num)
    : now_page_(num){}


  explicit TableIterator(Row *row,TableHeap *heap,page_id_t now_page)
    : row_(row),
      heap_(heap),
      now_page_(now_page){}

  // you may define your own constructor based on your member variables
  explicit TableIterator();

   TableIterator(const TableIterator &other);

  virtual ~TableIterator();

  inline bool operator==(const TableIterator &itr) const;

  inline bool operator!=(const TableIterator &itr) const;

  const Row &operator*();

  Row *operator->();

  TableIterator &operator++();

  TableIterator operator++(int);

private:
  Row *row_{nullptr};
  TableHeap *heap_;
  page_id_t now_page_;
  //MemHeap *mhp_;
  // add your own private member variables here
};

#endif //MINISQL_TABLE_ITERATOR_H
