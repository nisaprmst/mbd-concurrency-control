// Author: Kun Ren (kun.ren@yale.edu)
// Modified by Daniel Abadi

#include "txn/mvcc_storage.h"

// Init the storage
void MVCCStorage::InitStorage() {
  for (int i = 0; i < 1000000;i++) {
    Write(i, 0, 0);
    Mutex* key_mutex = new Mutex();
    mutexs_[i] = key_mutex;
  }
}

// Free memory.
MVCCStorage::~MVCCStorage() {
  for (unordered_map<Key, deque<Version*>*>::iterator it = mvcc_data_.begin();
       it != mvcc_data_.end(); ++it) {
    delete it->second;          
  }
  
  mvcc_data_.clear();
  
  for (unordered_map<Key, Mutex*>::iterator it = mutexs_.begin();
       it != mutexs_.end(); ++it) {
    delete it->second;          
  }
  
  mutexs_.clear();
}

// Lock the key to protect its version_list. Remember to lock the key when you read/update the version_list 
void MVCCStorage::Lock(Key key) {
  mutexs_[key]->Lock();
}

// Unlock the key.
void MVCCStorage::Unlock(Key key) {
  mutexs_[key]->Unlock();
}

// MVCC Read
bool MVCCStorage::Read(Key key, Value* result, int txn_unique_id) {
  // CPSC 438/538:
  //
  // Implement this method!
  
  if (mvcc_data_.count(key)) {

    // Hint: Iterate the version_lists and return the verion whose write timestamp
    // (version_id) is the largest write timestamp less than or equal to txn_unique_id.

    // get largest write timestamp version
    Version version;
    GetLargestWriteTimestamp(key, &version, txn_unique_id);

    // update result
    *result = version.value_;

    // success
    return true;
  }
  // if key not found
  return false;
}


// Check whether apply or abort the write
bool MVCCStorage::CheckWrite(Key key, int txn_unique_id) {
  // CPSC 438/538:
  //
  // Implement this method!
  
  // Hint: Before all writes are applied, we need to make sure that each write
  // can be safely applied based on MVCC timestamp ordering protocol. This method
  // only checks one key, so you should call this method for each key in the
  // write_set. Return true if this key passes the check, return false if not. 
  // Note that you don't have to call Lock(key) in this method, just
  // call Lock(key) before you call this method and call Unlock(key) afterward.

  if (mvcc_data_.count(key)) {
    // get largest write timestamp version
    Version version;
    GetLargestWriteTimestamp(key, &version, txn_unique_id);
    int read_timestamp = version.max_read_id_;
    // success
    if (txn_unique_id < read_timestamp) {
      return false;
    } else {
      return true;
    }
  }
  // if key not found
  return false;
}

// MVCC Write, call this method only if CheckWrite return true.
void MVCCStorage::Write(Key key, Value value, int txn_unique_id) {
  // CPSC 438/538:
  //
  // Implement this method!
  
  // Hint: Insert a new version (malloc a Version and specify its value/version_id/max_read_id)
  // into the version_lists. Note that InitStorage() also calls this method to init storage. 
  // Note that you don't have to call Lock(key) in this method, just
  // call Lock(key) before you call this method and call Unlock(key) afterward.
  
  if (mvcc_data_.count(key)) {
    // get largest write timestamp version
    Version version;
    GetLargestWriteTimestamp(key, &version, txn_unique_id);
    if (txn_unique_id == version.version_id_) {
      version.value_ = value;
    } else {
      Version *v = new Version;
      v->value_ = value;
      v->max_read_id_ = txn_unique_id;
      v->version_id_ = txn_unique_id;
      mvcc_data_[key]->push_back(v);
    }
  }
}

void MVCCStorage::GetLargestWriteTimestamp(Key key, Version* version, int txn_unique_id) {
  int curr_ver = 0;
  for (unsigned int i = 0; i < mvcc_data_[key]->size(); i++) {
    int ver = mvcc_data_[key]->at(i)->version_id_;
    if (ver <= txn_unique_id && ver > curr_ver) {
      curr_ver = ver;
      version = mvcc_data_[key]->at(i);
    }
  }
}