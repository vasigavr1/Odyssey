#include <iostream>
#include <fstream>
#include <assert.h>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <cstdarg>
#include <cassert>

using namespace std;

const uint word_entries = 3;
const bool print_read_words = true;
const uint keys_num = 1000;
const uint max_log_no = 10000;
const bool print_array = true;
const bool do_verbose_print = true;

void my_assert(bool cond, const char *format, ...) {
  if (!cond) {
    va_list argptr;
    va_start(argptr, format);
    vfprintf(stderr, format, argptr);
    va_end(argptr);
    assert(false);
  }
}

struct TextEntry {
    uint key;
    uint val;
    uint log_no;
    TextEntry() : key(0), val(0), log_no(0) {};
    void set_entry(uint word_index, uint word_val);

};

void TextEntry::set_entry(uint word_index, uint word_val)
{
  switch (word_index) {
    case 0:
      key =  word_val;
      if (print_read_words) cout << "key:" << key << endl;
      break;
    case 1:
      val = word_val;
      if (print_read_words) cout << "value:" << val << endl;
      break;
    case 2:
      log_no = word_val;
      if (print_read_words) cout << "log no:" << log_no << endl;
      break;
    default: assert(false);
  }
}

/*
 *    CONFLICT ARRAY -per key
 * */

struct ConfArrayEntry {
  bool valid;
  uint val;

//  TextEntry entry;
  ConfArrayEntry() : valid(false), val(0) {};
};


using conflict_vec = vector<ConfArrayEntry>;
class PerKeyArray {
  private:
    conflict_vec vec;
    uint size;
    uint key;
    bool valid;
  public:
     uint biggest_log_no;
    explicit PerKeyArray(uint new_key) : vec(max_log_no), size(max_log_no),
                                         key(new_key), valid(false), biggest_log_no(0) {};
    void insert_entry(TextEntry &new_entry);
    const bool is_valid() const {return valid;}
    const ConfArrayEntry& get_entry(uint i) const {return vec.at(i);}


};

void PerKeyArray::insert_entry(TextEntry &new_entry)
{
  uint new_log_no = new_entry.log_no;
  assert(new_entry.key == key);
  if (new_entry.log_no > size) {
    // extend the vector
  }
  assert(size >= new_log_no);

  // error, entry is taken
  if (vec[new_log_no].valid)
    my_assert(false, "For Key %u log %u is already taken, with val %u, "
      "new val: % \n", key, new_log_no, vec[new_log_no].val, new_entry.val);

  if (new_log_no > biggest_log_no) biggest_log_no = new_log_no;
  valid = true; // the key has been seen
  vec[new_log_no].valid = true;
  vec[new_log_no].val = new_entry.val;
}

/*
 *    ALL KEYS ARRAY -big data structure
 * */

using all_keys_vec = vector<PerKeyArray>;
class AllKeysArray {
  private:
    all_keys_vec vec;
    uint size;
    uint biggest_key_used;
  public:
    AllKeysArray();
    void insert_entry(TextEntry &new_entry);
    void printAllKeys(bool verbose) const;
};

AllKeysArray::AllKeysArray(): size(keys_num), biggest_key_used(0)
{
  vec.reserve(keys_num);
  for (uint i = 0; i < keys_num; i++) {
    vec.emplace_back(PerKeyArray(i));
  }
}

void AllKeysArray::insert_entry(TextEntry &new_entry)
{
  uint new_key = new_entry.key;
  if (new_key > size) {
    // extend the vector
  }
  assert(size >= new_key);
  if (new_key > biggest_key_used) biggest_key_used = new_key;
  vec.at(new_key).insert_entry(new_entry);
}

void AllKeysArray::printAllKeys(bool verbose) const {

  for (uint key_i = 0; key_i < biggest_key_used; key_i++) {
    cout << "Key: " << key_i;
    if (vec.at(key_i).is_valid()) cout << " valid, Biggest log_no: " <<  vec.at(key_i).biggest_log_no << endl;
    else cout << " not valid" << endl;
    if (verbose) {
      for (uint log_i = 0; log_i <= vec.at(key_i).biggest_log_no; log_i++) {
        if (vec.at(key_i).get_entry(log_i).valid) {
          cout << "\t Log: " << log_i << " val: " << vec.at(key_i).get_entry(log_i).val << endl;
        }
      }
    }
  }
}


int main()
{
  uint32_t thread_i = 0;
  AllKeysArray all_keys;
  while (true) {
    char file_name[50];
    sprintf(file_name, "thread%u.out", thread_i);
    ifstream file(file_name);
    if (file) cout << "Working on file: " << file_name << endl;
    else break;
    thread_i++;
    string word;
    uint word_i = 0;
    TextEntry entry;
    // TODO AN std:map to translate the keys 
    while (file >> word) {
      int word_val = stoi(word);
      uint word_index = word_i % word_entries;
      entry.set_entry(word_index, (uint) word_val);
      if (word_index == word_entries - 1) { // last word in sentence
        all_keys.insert_entry(entry);
      }
      word_i++;
    }
  }
  all_keys.printAllKeys(true);
  cout << "Done up to thread " << thread_i << std::endl;
  return 0;
}