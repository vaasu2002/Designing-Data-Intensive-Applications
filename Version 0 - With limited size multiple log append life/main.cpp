#include <iostream>
#include <fstream>
#include <string>
#include <map>
#include <thread>
#include <vector>
#include <memory>
#include <list>

// An append-only, log-structured key–value store with per-file in-memory indexing.
//
// Design:
//   - O(1) writes via sequential log appends
//   - O(1) reads using in-memory hash indexes
//   - Durable storage with crash recovery by replaying log files
//   - Automatic log rotation when a file reaches a fixed size limit
//
// Each log file maintains its own in-memory cache that maps keys to their
// corresponding byte offsets and record sizes within that file. This allows
// fast lookups without scanning disk contents.
//
// Records are written sequentially to disk, ensuring efficient writes and
// durability. On startup, each log file is replayed to rebuild its cache.
// When the active log file exceeds the configured size threshold, a new log
// file is created and becomes the active write target, while older files
// remain readable.

void log(const std::string& msg) {
    std::cout << "LOG: " << msg << "\n";
}

const char DELIMITER = '\0';
const uint64_t MAX_FILE_BYTE_SIZE = 20;

struct MetaData {
    uint64_t byteOffset;
    uint64_t byteSize;
};

class HashMap : public std::map<int, MetaData> {
private:
    uint64_t currByteOffset = 0;

public:
    HashMap() = default;

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
    std::unique_ptr<HashMap> cache;
    size_t totalBytes;
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
            cache->add(key, recordSize);
            currentOffset += recordSize;
        } // while(in)
        totalBytes = currentOffset;
        in.close();
        open();
    }
public:
    Store(const std::string& dir)
        : currDir(dir), cache(nullptr), totalBytes(0) {
        cache = std::make_unique<HashMap>();
        init();
    }

    bool set(int key, const std::string& data, size_t bytes) {
        if (!store.is_open()) {
            return false;
        }
        store << data;
        totalBytes += bytes;
        store.flush(); // write into disk
        cache->add(key, bytes);
        return true;
    }

    bool get(int key, std::string& out) const {
        MetaData metaData = cache->get(key);
        if (metaData.byteSize <= 0) return false;

        std::ifstream in(currDir);
        if (!in.is_open()) return false;

        in.seekg(metaData.byteOffset, std::ios::beg);
        std::getline(in, out, DELIMITER);
        return true;
    }

    size_t getTotalBytes() const {
        return totalBytes;
    }
};

class StorageEngine {
    // Note: Should contain current storage at 0th index (so do push front)
    std::list<std::unique_ptr<Store>> activeStores;
    std::vector<std::unique_ptr<Store>> archivedStores;
    std::string prefixFileName;
    std::string readBuffer;
    size_t totalFiles;
    size_t totalMerged;

    void init() {
        activeStores.clear();
        archivedStores.clear();
        // todo: upload history
        createStore();
    }

    void createStore() {
        totalFiles += 1;
        const std::string dir = prefixFileName + "_" + std::to_string(totalFiles) + ".txt";
        std::unique_ptr<Store> store = std::make_unique<Store>(dir);
        activeStores.push_front(std::move(store));
        log("Create a storage object with name: " + dir);
        return;
    }

    void onCapacityExceeded() {
        createStore();
    }
public:
    StorageEngine(const std::string& prefixFileName) : prefixFileName(prefixFileName),
        totalFiles(0), totalMerged(0) {
        init();
    }

    bool set(int key, const std::string& value) {

        // Making storage data
        std::string data = std::to_string(key) + "," + value + DELIMITER;
        size_t bytes = data.size();
        
        Store& currStore = *activeStores.front();

        // Checking if store capacity is exceeded
        if (currStore.getTotalBytes() + bytes > MAX_FILE_BYTE_SIZE) {
            onCapacityExceeded();
        }

        return currStore.set(key, data, bytes);
    }

    // Might need to lock it during the merging (or do we)?
    // Maybe just do when saving the merge file
    const char* get(int key) {
        readBuffer.clear();

        for (const auto& store : activeStores) {
            if (store->get(key, readBuffer)) {
                return readBuffer.c_str();
            }
        }

        log("Key not found: " + std::to_string(key));
        return nullptr;
    }
};

int main() {
    std::unique_ptr<StorageEngine> database = std::make_unique<StorageEngine>("D:\\Personal\\store");

    for (int i = 0; i < 10; i++) {
        database->set(1 + i, "1" + std::to_string(i + 1));
        database->set(2 + i, "2" + std::to_string(i + 1));
        database->set(3 + i, "3" + std::to_string(i + 1));
    }
    std::printf(database->get(3));
    std::cout << "" << std::endl;
    std::printf(database->get(12));

    database->set(3, "vaasu");
    std::cout << "" << std::endl;
    std::printf(database->get(3));
    return 0;
}
