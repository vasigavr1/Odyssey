#include <iostream>
#include <fstream>
#include <assert.h>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <cstdarg>
#include <cassert>
#include <map>
#include <stdint.h>
#include <string>

using namespace std;

const uint word_entries = 3;
const bool print_read_words = false;
const uint keys_num = 15000;
const uint max_log_no = 100000;
const uint malignant_log_gap_threshold = 50;
const bool print_array = false;
const bool do_verbose_print = true;

map<uint64_t, uint> key_map;
uint64_t index_to_key[keys_num] = {0};

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
    bool insert_entry_and_find_duplicates(TextEntry &new_entry);
    const bool is_valid() const {return valid;}
    const ConfArrayEntry& get_entry(uint i) const {return vec.at(i);}


};

// Returns true if it finds a duplicate
bool PerKeyArray::insert_entry_and_find_duplicates(TextEntry &new_entry)
{
  uint new_log_no = new_entry.log_no;
  assert(new_entry.key == key);
  if (new_entry.log_no >= size) {
    // extend the vector
    //cout << "Extending log number for key " << new_entry.key
    //     << ": New log " << new_log_no << " vec size: " << size
    //     << " biggest log no: " << biggest_log_no << endl;
    vec.resize(size + max_log_no);
    size = size + max_log_no;
  }

  my_assert(size >= new_log_no, "Log number too big: %u/%u ", new_log_no, size);

  // error, entry is taken
  if (vec[new_log_no].valid) {
    printf("For Key %u log %u is already taken, with val %u, "
             "new val: %u , key bkt %lu \n", key, new_log_no, vec[new_log_no].val, new_entry.val, index_to_key[key]);
    return true;
  }

  if (new_log_no > biggest_log_no) biggest_log_no = new_log_no;
  valid = true; // the key has been seen
  vec[new_log_no].valid = true;
  vec[new_log_no].val = new_entry.val;
  return false;
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
    uint log_duplicates;
    uint log_holes;
    uint malignant_log_holes;
    AllKeysArray();
    void insert_entry(TextEntry &new_entry);
    void printAllKeys(bool verbose) const;
    void check_for_holes_in_logs() ;
};

AllKeysArray::AllKeysArray(): size(keys_num), biggest_key_used(0),
                              log_duplicates(0), log_holes(0), malignant_log_holes(0)
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
  if (vec.at(new_key).insert_entry_and_find_duplicates(new_entry)) log_duplicates++;
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

void AllKeysArray::check_for_holes_in_logs() {

  for (uint key_i = 0; key_i < biggest_key_used; key_i++) {
    //cout << "Key: " << key_i;
    if (!vec.at(key_i).is_valid())
      cout << "Key: " << key_i << " bucket :" << index_to_key[key_i] <<" was not found " << endl;
    else {
      if (vec.at(key_i).biggest_log_no < max_log_no)
        cout << " Key: " << key_i << "has biggest log_no: " << vec.at(key_i).biggest_log_no << endl;
      for (uint log_i = 1; log_i <= vec.at(key_i).biggest_log_no; log_i++) {
        if (!vec.at(key_i).get_entry(log_i).valid) {
          log_holes++;
          if (vec.at(key_i).biggest_log_no - log_i > malignant_log_gap_threshold) {
            malignant_log_holes++;
            cout << " Key: " << key_i << " bucket :" << index_to_key[key_i] << " log gap in log: "
                 << log_i << " biggest log no: " << vec.at(key_i).biggest_log_no << endl;
          }
        }
      }

    }
  }
}


int main()
{
  uint32_t thread_i = 0;
  AllKeysArray all_keys;
  uint64_t total_file_lines_parsed = 0;
  uint32_t lines_no = 0;

  uint32_t keys_encountered = 0;
  while (true) {
    total_file_lines_parsed += lines_no;
    lines_no = 0;
    char file_name[50];
    sprintf(file_name, "logs/thread%u.out", thread_i);
    ifstream file(file_name);
    if (file) cout << "Working on file: " << file_name << endl;
    else break;
    thread_i++;
    string word;
    uint word_i = 0;
    TextEntry entry;
    while (file >> word) {
      uint64_t word_val;
      try {
        word_val = stoul(word);
      }
      catch(...) {
        printf("Stoul() throws an exception in file line %u: I consider this a file-related error "
                 "that has nothing to do with Paxos and move to the next file\n",
              lines_no);
        break;
      }
      uint word_index = word_i % word_entries;

      if (word_index == 0) {
        if (key_map.find(word_val) == key_map.end()) { // key not found
          key_map.insert(make_pair(word_val, keys_encountered));
          index_to_key[keys_encountered] = word_val;
          if (keys_encountered >= keys_num) {
            printf("Encountered %u keys, key bkt %lu in line %u: I consider this a file-related error "
                     "that has nothing to do with Paxos and move to the next file\n",
                   keys_encountered, word_val, lines_no);
            break;
          }
          word_val = keys_encountered;
          keys_encountered++;
          assert(key_map.size() == keys_encountered);

        }
        else {
          uint32_t key_index = key_map[word_val];
          assert(index_to_key[key_index] == word_val);
          word_val = key_index;
        }
      }
      if (word_index == 2) {// log
        if (word_val >= 10 * 100000) {
          printf("Encountered log no %lu in line %u: I consider this a file-related error "
                   "that has nothing to do with Paxos and move to the next file\n", word_val, lines_no);
          break;
        }
      }

      entry.set_entry(word_index, (uint32_t) word_val);
      if (word_index == word_entries - 1) { // last word in sentence
        lines_no++;
        all_keys.insert_entry(entry);
      }
      word_i++;
    }
  }
  if (print_array)
    all_keys.printAllKeys(do_verbose_print);

  all_keys.check_for_holes_in_logs();
  cout << "Done up to thread " << thread_i << endl
       << "Duplicates found:" << all_keys.log_duplicates << endl
       << "Holes found: " << all_keys.log_holes << endl
       << "of which malignant are " << all_keys.malignant_log_holes << endl
       << "Total file lines parsed: " << total_file_lines_parsed << endl;
  return 0;
}