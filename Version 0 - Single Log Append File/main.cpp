#include <iostream>
#include <fstream>
#include <string>
#include <map>

// A simple, append-only key-value store backed by a disk file (log) with in-memory 
// caching for fast lookups. Perfect for scenarios where you want:
//   - Read: O(1)
//   - Write: O(1)
//   - Crash recovery via replay of log
//   - All data appended in a single file, there is disk of going out of disk space.

const char DELIMITER = '\0';

struct MetaData {
    uint64_t byteOffset;
    uint64_t byteSize;
};

class HashMap : public std::map<int, MetaData> {
private:
    uint64_t currByteOffset = 0;
    HashMap() = default;

public:
    static HashMap& getInstance() {
        static HashMap instance;
        return instance;
    }

    uint64_t currentOffset() const {
        return currByteOffset;
    }

    void add(int key, uint64_t recordSize) {
        (*this)[key] = { currByteOffset, recordSize };
        currByteOffset += recordSize;
    }

    MetaData get(int key) const {
        auto it = find(key);
        if (it == end()){
            return { 0,0 };
        }
        return it->second;
    }

    void reset() {
        currByteOffset = 0;
        clear();
    }
};

class Store {
    std::ofstream store;
    std::string currDir;
    HashMap& cache;
    size_t files;
    void open() {
        store.open(currDir, std::ios::app);
    }
    void init() {
        // Make sure file is closed before reading
        if (store.is_open()) {
            store.close();
        }
        
        std::ifstream in(currDir, std::ios::binary);
        if (!in.is_open()) {
            // File doesn't exist yet means it a fresh start
            // nothing to recover
            open();
            return;
        }

        uint64_t currentOffset = 0;
        
        while (in) {
            uint64_t startPos = in.tellg();
            std::string data;
            // Read until delimiter
            std::getline(in, data, DELIMITER);

            if (data.empty() && in.eof()) break;
            if (data.empty()) continue;

            // Parse key,value
            size_t commaPos = data.find(',');
            if (commaPos == std::string::npos) {
                // Skip corrupted record
                currentOffset = in.tellg();
                continue;
            }

            std::string keyStr = data.substr(0, commaPos);
            std::string value = data.substr(commaPos + 1);

            int key;
            try {
                key = std::stoi(keyStr);
            }
            catch (...) {
                // Skip bad key
                currentOffset = in.tellg();
                continue;
            }

            uint64_t recordSize = in.tellg();
            recordSize -= startPos;
            cache.add(key, recordSize);
            currentOffset += recordSize;
        } // while(in)
        in.close();
        open();
    }
public:
    Store(const std::string& dir)
        : currDir(dir), cache(HashMap::getInstance()) {
        init();
    }

    bool set(int key, const std::string& value) {
        if (!store.is_open()) return false;

        std::string data = std::to_string(key) + "," + value + DELIMITER;
        store << data;
        store.flush(); // write
        cache.add(key, data.size());

        return true;
    }

    bool get(int key, std::string& out) const {
        MetaData metaData = cache.get(key);
        if (metaData.byteSize <= 0) return false;

        std::ifstream in(currDir);
        if (!in.is_open()) return false;

        in.seekg(metaData.byteOffset, std::ios::beg);
        std::getline(in, out, DELIMITER);
        return true;
    }
};

int main() {
    Store store("store.txt");

    store.set(11, "11");
    store.set(12, "12-1");
    store.set(12, "12-2");

    std::string output;
    if (store.get(11, output)) {
        std::cout << output << std::endl;
    }
    else {
        std::cout << "not found" << std::endl;
    }
    if (store.get(12, output)) {
        std::cout << output << std::endl;
    }
    else {
        std::cout << "not found" << std::endl;
    }
    return 0;
}
