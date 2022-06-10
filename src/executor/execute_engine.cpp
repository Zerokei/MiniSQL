#include "executor/execute_engine.h"
#include "glog/logging.h"
using namespace std;

ExecuteEngine::ExecuteEngine() {
  dbs_.clear();
  current_db_ = "";
  if(use_local_file) {
    file_io_.open("db_name_file.txt", ios :: in);
    if(!file_io_.is_open()) {
      file_io_.open("db_name_file.txt", ios :: out);
      file_io_.close();
      return ;
    }
    string db_name;
    while(file_io_ >> db_name) {
      cout << "Initially add database " << db_name << "!" << endl;
      DBStorageEngine *new_engine = new DBStorageEngine(db_name, false);
      dbs_.insert(make_pair(db_name, new_engine));
    }
    file_io_.close();
  }
}

Field GetField(TypeId tid, char *val) {
  if(tid == kTypeInt) return Field(kTypeInt, (int32_t)atoi(val));
  else if(tid == kTypeFloat) return Field(kTypeFloat, (float)atof(val));
  else if(tid == kTypeChar) return Field(kTypeChar, val, strlen(val) + 1, true);
  else return Field(tid);
}
void signal(int c) {
  cerr << " !!!!!!!! " << c << endl;
}
dberr_t ExecuteEngine::Execute(pSyntaxNode ast, ExecuteContext *context) {
  if(ast == nullptr) return DB_FAILED;
  if(current_db_ != "") {
      dbs_[current_db_]->bpm_->CheckAllUnpinned();
  }
  message_ = &context->message_;
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context);
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context);
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context);
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context);
    case kNodeShowTables:
      return ExecuteShowTables(ast, context);
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context);
    case kNodeDropTable:
      return ExecuteDropTable(ast, context);
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context);
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context);
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context);
    case kNodeSelect:
      return ExecuteSelect(ast, context);
    case kNodeInsert:
      return ExecuteInsert(ast, context);
    case kNodeDelete:
      return ExecuteDelete(ast, context);
    case kNodeUpdate:
      return ExecuteUpdate(ast, context);
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context);
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context);
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context);
    case kNodeExecFile:
      return ExecuteExecfile(ast, context);
    case kNodeQuit:
      return ExecuteQuit(ast, context);
    default:
      break;
  }
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if(dbs_.find(db_name) != dbs_.end()) {
    *message_ += "Error: Database " + db_name + " exists!\n";
    return DB_FAILED;
  }
  DBStorageEngine *new_engine = new DBStorageEngine(db_name);
  dbs_.insert(make_pair(db_name, new_engine));
  if(use_local_file) {
    file_io_.open("db_name_file.txt", ios :: app);
    file_io_ << db_name << endl;
    file_io_.close();
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if(dbs_.find(db_name) == dbs_.end()) {
    *message_ += "Error: Database " + db_name + " does not exist!\n";
    return DB_FAILED;
  }
  delete dbs_.find(db_name)->second;
  dbs_.erase(db_name);
  if(db_name == current_db_) current_db_ = "";
  if(use_local_file) {
    file_io_.open("db_name_file.txt", ios :: out);
    for(auto it: dbs_) file_io_ << it.first << endl;
    file_io_.close();
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  *message_ += "Databases: ";
  for(auto it: dbs_) *message_ += it.first + " ";
  *message_ += "\nCurrent database: " + current_db_ + "\n";
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) == dbs_.end()) {
    *message_ += "Error: Database " + db_name + " does not exist!\n";
    return DB_FAILED;
  }
  current_db_ = db_name;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  if(current_db_ == "") {
    *message_ += "Error: No database being used!\n";
    return DB_FAILED;
  }
  vector<TableInfo *> tables;
  dbs_[current_db_]->catalog_mgr_->GetTables(tables);
  *message_ += "Tables: ";
  for(auto it: tables) *message_ += it->GetTableName() + " ";
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  if(current_db_ == "") {
    *message_ += "Error: No database being used!\n";
    return DB_FAILED;
  }
  ast = ast->child_;
  string table_name = ast->val_;
  TableInfo *temp;
  if(dbs_[current_db_]->catalog_mgr_->GetTable(table_name, temp) == DB_SUCCESS) {
    *message_ += "Error: Table " + table_name + " exists!\n";
    return DB_FAILED;
  }
  ast = ast->next_->child_;
  vector<Column *> columns;
  SimpleMemHeap temp_heap;
  int cnt = 0;
  while(ast != nullptr && ast->type_ == kNodeColumnDefinition) {
    TypeId tid = Type :: GetTid(ast->child_->next_->val_);
    
    int length = 0;
    if(tid == kTypeChar) {
      string str = ast->child_->next_->child_->val_;
      if(str.find('.') != str.npos) {
        *message_ += "Error: Illegal char length!\n";
        return DB_FAILED;
      }
      length = atoi(str.c_str());
      if(length < 0) {
        *message_ += "Error: Illegal char length!\n";
        return DB_FAILED;
      }
    }
    bool is_unique = false;
    if(ast->val_ != nullptr && ast->val_[0] == 'u') is_unique = true;

    if(tid == kTypeChar) columns.push_back(ALLOC_COLUMN(temp_heap)(ast->child_->val_, tid, length, cnt, true, is_unique));
    else columns.push_back(ALLOC_COLUMN(temp_heap)(ast->child_->val_, tid, cnt, true, is_unique));

    ast = ast->next_;
    cnt++;
  }
  Schema schema(columns);
  TableInfo *table_info; 
  if(dbs_[current_db_]->catalog_mgr_->CreateTable(table_name, &schema, nullptr, table_info) != DB_SUCCESS) {
    *message_ += "Error: Create table " + table_name + " failed!\n";
    return DB_FAILED;
  }

  if(ast != nullptr) {
    string primary_index_name = "primary_index_" + table_name;
    vector<string> primary_keys;
    auto pos = ast->child_;
    while(pos != nullptr) {
      primary_keys.push_back(pos->val_);
      pos = pos->next_;
    }
    IndexInfo *temp;
    if(dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name, primary_index_name, primary_keys, nullptr, temp) !=DB_SUCCESS) {
      *message_ += "Error: Create table " + table_name + " primary index failed!\n";
      return DB_FAILED;
    }
  }
  for(auto col : table_info->GetSchema()->GetColumns()) {
    if(col->IsUnique()) {
      string unique_key = col->GetName();
      string unique_index_name = "unique_index_" + table_name + "_" + unique_key;
      IndexInfo *temp;
      if(dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name, unique_index_name, {unique_key}, nullptr, temp) != DB_SUCCESS) {
        *message_ += "Error: Create key " + unique_key + " unique index failed!\n";
        return DB_FAILED;
      }
    }
  }
  return DB_SUCCESS;
}
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  if(current_db_ == "") {
    *message_ += "Error: No database being used!\n";
    return DB_FAILED;
  }
  string table_name = ast->child_->val_;
  TableInfo *temp;
  if(dbs_[current_db_]->catalog_mgr_->GetTable(table_name, temp) == DB_TABLE_NOT_EXIST) {
    *message_ += "Error: Table " + table_name + " does not exist!\n";
    return DB_FAILED;
  }
  // signal(0);
  return dbs_[current_db_]->catalog_mgr_->DropTable(table_name);
}
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  if(current_db_ == "") {
    *message_ += "Error: No database being used!\n";
    return DB_FAILED;
  }
  vector<TableInfo *> tables;
  vector<IndexInfo *> indexes;
  dbs_[current_db_]->catalog_mgr_->GetTables(tables);
  for(auto table: tables) {
    indexes.clear();
    *message_ += "Table name: " + table->GetTableName() + "\nIndex name: ";
    dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table->GetTableName(), indexes);
    for(auto index: indexes) *message_ += index->GetIndexName() + " ";
    *message_ += "\n";
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  if(current_db_ == "") {
    *message_ += "Error: No database being used!\n";
    return DB_FAILED;
  }
  string table_name = ast->child_->next_->val_;
  string index_name = ast->child_->val_;
  vector<string> index_keys;
  auto pos = ast->child_->next_->next_->child_;
  while(pos != nullptr) {
    index_keys.push_back(pos->val_);
    pos = pos->next_;
  }
  IndexInfo *index_info;
  dberr_t status = dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name, index_name, index_keys, nullptr, index_info);
  if(status == DB_TABLE_NOT_EXIST) {
    *message_ += "Error: Table " + table_name + " does not exist!\n";
    return DB_FAILED;
  } else if(status == DB_INDEX_ALREADY_EXIST) {
    *message_ += "Error: Index " + index_name + " already exists!\n";
    return DB_FAILED;
  }
  TableInfo *table_info;
  dbs_[current_db_]->catalog_mgr_->GetTable(table_name, table_info);
  TableHeap *table_heap = table_info->GetTableHeap();
  for(auto it = table_heap->Begin(); it != table_heap->End(); it++) {
    Row row = *it;
    vector<Field> fields;
    for(uint32_t i = 0; i < index_info->GetIndexKeySchema()->GetColumnCount(); i++) {
      fields.push_back(*row.GetField(index_info->GetIndexKeySchema()->GetColumn(i)->GetTableInd()));
    }
    Row key(fields);
    if(index_info->GetIndex()->InsertEntry(key, row.GetRowId(), nullptr) != DB_SUCCESS) {
      *message_ += "Error: Creating Index " + index_name + "will cause duplicate tuples!\n";
      dbs_[current_db_]->catalog_mgr_->DropIndex(table_name, index_name);
      return DB_FAILED;
    }
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  if(current_db_ == "") {
    *message_ += "Error: No database being used!\n";
    return DB_FAILED;
  }
  string index_name = ast->child_->val_;
  vector<TableInfo *> table_infos;
  dbs_[current_db_]->catalog_mgr_->GetTables(table_infos);
  for(auto table_info: table_infos) 
    dbs_[current_db_]->catalog_mgr_->DropIndex(table_info->GetTableName(), index_name);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteSelect(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteSelect" << std::endl;
#endif
  if(current_db_ == "") {
    *message_ += "Error: No database being used!\n";
    return DB_FAILED;
  }
  string table_name = ast->child_->next_->val_;
  TableInfo *table_info;
  if(dbs_[current_db_]->catalog_mgr_->GetTable(table_name, table_info) != DB_SUCCESS) {
    *message_ += "Error: Table " + table_name + " does not exist!\n";
    return DB_FAILED;
  }
  vector<IndexInfo *> index_infos;
  dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table_name, index_infos);

  vector<Row> rows;
  if(GetRows(ast->child_->next_->next_, table_info, index_infos, &rows) != DB_SUCCESS) {
    return DB_FAILED;
  }
  vector<Row> projection_rows;
  if(ast->child_->type_ == kNodeAllColumns) {
    for(auto &row: rows) projection_rows.emplace_back(row);
  } else {
    Schema *schema = table_info->GetSchema();
    for(auto &row: rows) {
      vector<Field> projection_fields;
      auto pos = ast->child_->child_;
      while(pos != nullptr) {
        string column_name = pos->val_;
        uint32_t column_indexex;
        if (schema->GetColumnIndex(column_name, column_indexex) == DB_COLUMN_NAME_NOT_EXIST) {
          *message_ += "Error: Column " + column_name + " does not exist!\n";
          return DB_FAILED;
        }
        projection_fields.push_back(*row.GetField(column_indexex));
        pos = pos->next_;
      }
      projection_rows.push_back(Row(projection_fields));
    }
  }
  *message_ += "Tuple: \n";
  for(auto it: projection_rows) {
    vector<Field *> fields = it.GetFields();
    for(int i = 0; i < (int)it.GetFieldCount(); ++i) {
      *message_ += (i == 0?"(":", ") + fields[i]->GetString();
    }
    *message_ += ")\n";
  }
  *message_ += "select " + to_string(projection_rows.size()) + " tuples.\n";
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteInsert(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteInsert" << std::endl;
#endif
  if(current_db_ == "") {
    *message_ += "Error: No database being used!\n";
    return DB_FAILED;
  }
  string table_name;
  table_name = ast->child_->val_;
  TableInfo *table_info;
  if (dbs_[current_db_]->catalog_mgr_->GetTable(table_name, table_info) != DB_SUCCESS) {
    *message_ += "Error: Table " + table_name + " does not exist!\n";
    return DB_FAILED;
  }
  Schema *schema = table_info->GetSchema();
  auto pos = ast->child_->next_->child_;
  vector<Field> fields;

  uint32_t cnt = 0;
  while(pos != nullptr) {
    fields.push_back(GetField(schema->GetColumn(cnt)->GetType(), pos->val_));
    pos = pos->next_;
    cnt++;
  }
  // signal(3);
  if(cnt != schema->GetColumnCount()) {
    *message_ += "Error: Invaild tuple!\n";
    return DB_FAILED;
  }
  Row row(fields);

  vector<IndexInfo *> index_infos;
  dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table_name, index_infos);

  for(auto it: index_infos) {
    vector<Field> keys;
    for(uint32_t i = 0; i < it->GetIndexKeySchema()->GetColumnCount(); i++) {
      keys.push_back(*row.GetField(it->GetIndexKeySchema()->GetColumn(i)->GetTableInd()));
    }
    vector<RowId> temp;
    if(it->GetIndex()->ScanKey(Row(keys), temp, nullptr) != DB_KEY_NOT_FOUND) {
      *message_ += "Error: duplicate tuples!\n";
      return DB_FAILED;
    }
  }
  // signal(4);
  if(!table_info->GetTableHeap()->InsertTuple(row, nullptr)) {
    *message_ += "Error: insert the tuple failed!\n";
    return DB_FAILED;
  }
  // signal(6);
  index_infos.clear();
  dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table_name, index_infos);
  for(auto it: index_infos) {
    vector<Field> keys;
    for(uint32_t i = 0; i < it->GetIndexKeySchema()->GetColumnCount(); i++) {
      keys.push_back(*row.GetField(it->GetIndexKeySchema()->GetColumn(i)->GetTableInd()));
    }
    Row key(keys);
    key.SetRowId(row.GetRowId());
    if(it->GetIndex()->InsertEntry(key, key.GetRowId(), nullptr) != DB_SUCCESS) {
      *message_ += "Error: Insert index failed!\n";
      return DB_FAILED;
    }
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDelete(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDelete" << std::endl;
#endif
  if(current_db_ == "") {
    *message_ += "Error: No database being used!\n";
    return DB_FAILED;
  }
  string table_name = ast->child_->val_;
  TableInfo *table_info;
  if(dbs_[current_db_]->catalog_mgr_->GetTable(table_name, table_info) != DB_SUCCESS) {
    *message_ += "Error: Table " + table_name + " does not exist!\n";
    return DB_FAILED;
  }
  vector<IndexInfo *> index_infos;
  dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table_name, index_infos);

  vector<Row> rows;
  if(GetRows(ast->child_->next_, table_info, index_infos, &rows) != DB_SUCCESS) {
    return DB_FAILED;
  }

  for(auto &row : rows) {
    if(!table_info->GetTableHeap()->MarkDelete(row.GetRowId(), nullptr)) {
      *message_ += "Error: delete tuple failed!\n";
      return DB_FAILED;
    }
    for(auto it: index_infos) {
      vector<Field> keys;
      for(uint32_t i = 0; i < it->GetIndexKeySchema()->GetColumnCount(); i++) {
        keys.push_back(*row.GetField(it->GetIndexKeySchema()->GetColumn(i)->GetTableInd()));
      }
      Row key(keys);
      if(it->GetIndex()->RemoveEntry(key, key.GetRowId(), nullptr) != DB_SUCCESS) {
        *message_ += "Error: Remove index key failed!\n";
        return DB_FAILED;
      }
    }
    table_info->GetTableHeap()->ApplyDelete(row.GetRowId(), nullptr);
  }
  *message_ += "delete " + to_string(rows.size()) + " tuples\n";
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUpdate" << std::endl;
#endif
  if(current_db_ == "") {
    *message_ += "Error: No database being used!\n";
    return DB_FAILED;
  }
  string table_name = ast->child_->val_;
  TableInfo *table_info;
  if(dbs_[current_db_]->catalog_mgr_->GetTable(table_name, table_info) != DB_SUCCESS) {
    *message_ += "Error: Table " + table_name + " does not exist!\n";
    return DB_FAILED;
  }
  Schema *schema = table_info->GetSchema();
  vector<IndexInfo *> index_infos;
  dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table_name, index_infos);

  vector<Row> rows;
  if(GetRows(ast->child_->next_->next_, table_info, index_infos, &rows) != DB_SUCCESS) {
    return DB_FAILED;
  }

  vector<char *> val(schema->GetColumnCount(), nullptr);
  auto pos = ast->child_->next_;
  pos = pos->child_;
  while (pos != nullptr) {
    uint32_t column_index;
    string column_name = pos->child_->val_;
    if(schema->GetColumnIndex(column_name, column_index) == DB_COLUMN_NAME_NOT_EXIST) {
      *message_ += "Error: Column " + column_name + " does not exist!\n";
      return DB_FAILED;
    }
    val[column_index] = pos->child_->next_->val_;
    pos = pos->next_;
  }

  vector<Row> upd_rows;
  for(auto row : rows) {
    auto fields = row.GetFields();
    vector<Field> upd_fields;
    for(uint32_t i = 0; i < row.GetFieldCount(); i++) {
      if(val[i] != nullptr) upd_fields.push_back(GetField(schema->GetColumn(i)->GetType(), val[i]));
      else upd_fields.emplace_back(*fields[i]);
    }
    Row upd_row(upd_fields);
    upd_row.SetRowId(row.GetRowId());
    upd_rows.emplace_back(upd_row);
  }

  dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table_name, index_infos);
  for(auto upd_row : upd_rows) {
    for(auto index_info: index_infos) {
      vector<Field> key_fields;
      for(uint32_t i = 0; i < index_info->GetIndexKeySchema()->GetColumnCount(); i++) {
        key_fields.push_back(*upd_row.GetField(index_info->GetIndexKeySchema()->GetColumn(i)->GetTableInd()));
      }
      Row key(key_fields);
      vector<RowId> row_ids;
      if(index_info->GetIndex()->ScanKey(key, row_ids, nullptr) == DB_SUCCESS) {
        if(row_ids[0] == upd_row.GetRowId()) continue;
        *message_ += "Error: Updating cause duplicate tuples!\n";
        return DB_FAILED;
      }
    }
  }
  for(uint32_t i = 0; i < upd_rows.size(); i++) {
    Row upd_row = upd_rows[i];
    Row old_row = rows[i];
    table_info->GetTableHeap()->UpdateTuple(upd_row, old_row.GetRowId(), nullptr);
    for(auto index_info: index_infos) {
      vector<Field> old_key_fields;
      for (uint32_t i = 0; i < index_info->GetIndexKeySchema()->GetColumnCount(); i++) {
        old_key_fields.push_back(*old_row.GetField(index_info->GetIndexKeySchema()->GetColumn(i)->GetTableInd()));
      }
      Row old_key(old_key_fields);
      old_key.SetRowId(old_row.GetRowId());
      index_info->GetIndex()->RemoveEntry(old_key, old_key.GetRowId(), nullptr);
      vector<Field> new_key_fields;
      for (uint32_t i = 0; i < index_info->GetIndexKeySchema()->GetColumnCount(); i++) {
        new_key_fields.push_back(*upd_row.GetField(index_info->GetIndexKeySchema()->GetColumn(i)->GetTableInd()));
      }
      Row new_key(new_key_fields);
      new_key.SetRowId(upd_row.GetRowId());
      index_info->GetIndex()->InsertEntry(new_key, new_key.GetRowId(), nullptr);
    }
  }
  *message_ += "update " + to_string(upd_rows.size()) + " tuples.\n";
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  string file_name = ast->child_->val_;
  file_io_.open(file_name, ios :: in);
  if(!file_io_.is_open()) {
    *message_ += "Error: Open file failed!\n";
    return DB_FAILED;
  }
  const int buf_size = 1024;
  char cmd[buf_size];
  int cmd_cnt = 0;
  double tot_time = 0;
  while(true) {
    memset(cmd, 0, buf_size);
    bool end = false;
    char ch;
    int i = 0;
    while(true) {
      if(!file_io_.get(ch)) {
        end = true;
        break;
      }
      cmd[i++] = ch;
      if(ch == ';') break;
    }
    if(end) break;
    file_io_.get();

    YY_BUFFER_STATE bp = yy_scan_string(cmd);
    if (bp == nullptr) {
      LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
      exit(1);
    }
    yy_switch_to_buffer(bp);

    // init parser module
    MinisqlParserInit();

    // parse
    yyparse();

    // parse result handle
    if (MinisqlParserGetError()) {
      // error
      printf("%s\n", MinisqlParserGetErrorMessage());
    } else {
#ifdef ENABLE_PARSER_DEBUG
      printf("[INFO] Sql syntax parse ok!\n");
      SyntaxTreePrinter printer(MinisqlGetParserRootNode());
      primarynter.PrintTree(syntax_tree_file_mgr[0]);
#endif
    }

    clock_t start = clock();
    dberr_t status = Execute(MinisqlGetParserRootNode(), context);
    clock_t stop = clock();

    MinisqlParserFinish();
    yy_delete_buffer(bp);
    yylex_destroy();

    if(status != DB_SUCCESS) {
      file_io_.close();
      return DB_FAILED;
    } else {
      tot_time += (double)(stop - start) / CLOCKS_PER_SEC;
      cmd_cnt++;
    }
    if(context->flag_quit_) {
      break;
    }
    *message_ = "";
  }

  *message_ += to_string(cmd_cnt) + " commands use " + to_string(tot_time) + "sec.\n";
  
  file_io_.close();
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  context->flag_quit_ = true;
  return DB_SUCCESS;
}
dberr_t ExecuteEngine::GetRows(const pSyntaxNode ast, TableInfo *table_info,vector<IndexInfo *> index_infos, vector<Row> *rows) {
  TableHeap *table_heap = table_info->GetTableHeap();
  if(ast == nullptr) {
    for(auto it = table_heap->Begin(); it != table_heap->End(); ++it) 
      rows->emplace_back(*it);
    return DB_SUCCESS;
  }
  if(ast->child_->type_ == kNodeCompareOperator) {
    string column_name = ast->child_->child_->val_, comparator = ast->child_->val_;
    uint32_t column_index;
    if(table_info->GetSchema()->GetColumnIndex(column_name, column_index) == DB_COLUMN_NAME_NOT_EXIST) {
      *message_ += "Error: Column " + column_name + " does not exist!\n";
      return DB_FAILED;
    }
    for(auto index_info : index_infos) {
      if(index_info->GetIndexKeySchema()->GetColumnCount() == 1 && index_info->GetIndexKeySchema()->GetColumn(0)->GetName() == column_name && comparator == "=") {
        vector<Field> fields;
        fields.push_back(GetField(index_info->GetIndexKeySchema()->GetColumn(0)->GetType(), ast->child_->child_->next_->val_));
        Row key(fields);
        vector<RowId> row_ids;
        index_info->GetIndex()->ScanKey(key, row_ids, nullptr);
        for(auto it: row_ids) {
          Row row(it);
          table_heap->GetTuple(&row, nullptr);
          rows->push_back(row);
        }
        return DB_SUCCESS;
      }
    }
  }
  dberr_t status = DB_SUCCESS;
  for(auto it = table_heap->Begin(); it != table_heap->End(); it++) 
    if(CheckExpression(ast, *it, table_info, status))  
      rows->push_back(*it);
  return status;
}

bool ExecuteEngine::CheckExpression(pSyntaxNode ast, const Row &row, TableInfo *table_info, dberr_t &status) {
  if(ast->type_ == kNodeConditions) {
    return CheckExpression(ast->child_, row, table_info, status);
  } else if(ast->type_ == kNodeConnector)  {
    auto pos = ast->child_;
    string connector = ast->val_;
    if(connector == "and") {
      while(pos != nullptr) {
        if(!CheckExpression(pos, row, table_info, status))  return false;
        pos = pos->next_;
      }
      return true;
    } else if(connector == "or") {
      while(pos != nullptr) {
        if(CheckExpression(pos, row, table_info, status)) return true;
        pos = pos->next_;
      }
      return false;
    }
  } else if(ast->type_ == kNodeCompareOperator) {
    string column_name = ast->child_->val_;
    uint32_t column_index;
    if(table_info->GetSchema()->GetColumnIndex(column_name, column_index) == DB_COLUMN_NAME_NOT_EXIST) {
      *message_ += "Error: Column " + column_name + " does not exist!\n";
      return false;
    }
    Field *field = row.GetField(column_index);
    string comparator = ast->val_;
    Field temp = GetField(field->GetTypeId(), ast->child_->next_->val_); 
    if(comparator == "=") return field->CompareEquals(temp);
    else if(comparator == "<") return field->CompareLessThan(temp);
    else if(comparator == "<=") return field->CompareLessThanEquals(temp);
    else if(comparator == ">") return field->CompareGreaterThan(temp);
    else if(comparator == ">=") return field->CompareGreaterThanEquals(temp);
    else if(comparator == "<>") return field->CompareNotEquals(temp);
  }
  *message_ += "Error: Illegal expression!\n";
  status = DB_FAILED;
  return false;
}