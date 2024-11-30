#include <iostream>
#include <fstream>
#include <map>
#include <pthread.h>
#include <string>
#include <vector>
#include <queue>
#include <cctype>
#include <algorithm>

using namespace std;

// Represents metadata about a file (name and size)
struct FileBucket {
    string File;
    int Size;
};

// Represents a chunk of a file (portion to be processed)
struct Chunk {
    string File;
    int Start;  // Start byte
    int End;    // End byte
    int FileID; // Unique ID for the file
};

// Thread memory for mapping phase
struct MapperThreadMemory {
    int ThreadID;
    int MapperThreadsCount;
    vector<FileBucket>* FileBuckets;
    vector<map<string, vector<int>>>* PartialIndexes;
    pthread_barrier_t* MapperReducerBarrier;
    queue<Chunk>* ChunkQueue;
    pthread_mutex_t* ChunkMutex;
};

// Thread memory for reducing phase
struct ReducerThreadMemory {
    int ThreadID;
    int MapperThreadsCount;
    int ReducerThreadsCount;
    vector<map<string, vector<int>>>* PartialIndexes;
    pthread_barrier_t* MapperReducerBarrier;
    queue<map<string, vector<int>>>* ReduceQueue;
    pthread_mutex_t* ReduceMutex;
};

// Adds file chunks to the queue for processing
void AddChunksToQueue(const vector<FileBucket>& fileBuckets, int chunkSize, queue<Chunk>& chunkQueue) {
    for (int fileID = 0; fileID < fileBuckets.size(); fileID++) {
        const auto& file = fileBuckets[fileID];
        int start = 0;
        while (start < file.Size) {
            int end = min(start + chunkSize, file.Size);
            chunkQueue.push({file.File, start, end, fileID});
            start = end;
        }
    }
}

// Maps a chunk to an inverse index
void Map(Chunk chunk, map<string, vector<int>>& partialIndex) {
    ifstream fin(chunk.File);
    fin.seekg(chunk.Start);
    string word;

    int position = chunk.Start;
    while (position < chunk.End && fin >> word) {
        // Clean and normalize the word
        word.erase(remove_if(word.begin(), word.end(), [](char c) { return !isalpha(c); }), word.end());
        transform(word.begin(), word.end(), word.begin(), ::tolower);

        // Update the index
        if (!word.empty()) {
            if (find(partialIndex[word].begin(), partialIndex[word].end(), chunk.FileID) == partialIndex[word].end()) {
                partialIndex[word].push_back(chunk.FileID); // Record the file ID
            }
        }

        position = fin.tellg();
        if (position == -1) break; // EOF reached
    }
}

// Combines two maps
void CombineMaps(map<string, vector<int>>& map1, const map<string, vector<int>>& map2) {
    for (const auto& [word, fileIDs] : map2) {
        vector<int>& combinedFileIDs = map1[word];
        for (int fileID : fileIDs) {
            if (find(combinedFileIDs.begin(), combinedFileIDs.end(), fileID) == combinedFileIDs.end()) {
                combinedFileIDs.push_back(fileID);
            }
        }
    }
}

// Mapper thread function
void* MapThread(void* args) {
    MapperThreadMemory* threadMemory = (MapperThreadMemory*)args;

    while (true) {
        pthread_mutex_lock(threadMemory->ChunkMutex);
        if (threadMemory->ChunkQueue->empty()) {
            pthread_mutex_unlock(threadMemory->ChunkMutex);
            break; // No more chunks
        }
        Chunk chunk = threadMemory->ChunkQueue->front();
        threadMemory->ChunkQueue->pop();
        pthread_mutex_unlock(threadMemory->ChunkMutex);

        // Map the chunk
        Map(chunk, (*threadMemory->PartialIndexes)[threadMemory->ThreadID]);
    }

    // Barrier to synchronize threads after mapping phase
    pthread_barrier_wait(threadMemory->MapperReducerBarrier);
    return NULL;
}

// Reducer thread function
void* ReduceThread(void* args) {
    ReducerThreadMemory* threadMemory = (ReducerThreadMemory*)args;

    while (true) {
        map<string, vector<int>> map1, map2;

        pthread_mutex_lock(threadMemory->ReduceMutex);
        if (threadMemory->ReduceQueue->size() < 2) {
            pthread_mutex_unlock(threadMemory->ReduceMutex);
            break; // Not enough maps to process
        }

        // Get two maps from the queue
        map1 = move(threadMemory->ReduceQueue->front());
        threadMemory->ReduceQueue->pop();
        map2 = move(threadMemory->ReduceQueue->front());
        threadMemory->ReduceQueue->pop();
        pthread_mutex_unlock(threadMemory->ReduceMutex);

        // Combine the maps
        CombineMaps(map1, map2);

        // Push the combined map back into the queue
        pthread_mutex_lock(threadMemory->ReduceMutex);
        threadMemory->ReduceQueue->push(move(map1));
        pthread_mutex_unlock(threadMemory->ReduceMutex);
    }

    return NULL;
}

int main(int argc, char** argv) {
    if (argc != 4) {
        cerr << "Usage: " << argv[0] << " <no_mapper_threads> <no_reducer_threads> <input_file>" << endl;
        return 1;
    }

    int mapperThreadsCount = stoi(argv[1]);
    int reducerThreadsCount = stoi(argv[2]);
    string inputFile = argv[3];

    // Read the file metadata
    ifstream fin(inputFile);
    vector<FileBucket> fileBuckets;
    int n;
    fin >> n;
    for (int i = 0; i < n; i++) {
        string datasetName;
        fin >> datasetName;

        // Get file size
        ifstream fileStream(datasetName, ios::binary | ios::ate);
        int fileSize = fileStream.tellg();

        fileBuckets.push_back({datasetName, fileSize});
    }

    // Create a queue for file chunks
    queue<Chunk> chunkQueue;
    int chunkSize = 1024; // Define a chunk size
    AddChunksToQueue(fileBuckets, chunkSize, chunkQueue);

    // Initialize partial indexes for threads
    vector<map<string, vector<int>>> partialIndexes(mapperThreadsCount);

    // Create and initialize barrier for synchronizing mapping and reducing phases
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, nullptr, mapperThreadsCount);

    // Create a queue for reducing phase
    queue<map<string, vector<int>>> reduceQueue;

    // Initialize mutexes
    pthread_mutex_t chunkMutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t reduceMutex = PTHREAD_MUTEX_INITIALIZER;

    // Create all threads (both mapping and reducing)
    vector<pthread_t> threads(mapperThreadsCount + reducerThreadsCount);
    vector<MapperThreadMemory> mapperMemories(mapperThreadsCount);
    vector<ReducerThreadMemory> reducerMemories(reducerThreadsCount);

    for (int i = 0; i < mapperThreadsCount + reducerThreadsCount; i++) {
        if (i < mapperThreadsCount) {
            mapperMemories[i] = {i, mapperThreadsCount, &fileBuckets, &partialIndexes, &barrier, &chunkQueue, &chunkMutex};
            pthread_create(&threads[i], nullptr, MapThread, (void*)&mapperMemories[i]);
        } else {
            int idx = i - mapperThreadsCount;
            reducerMemories[idx] = {idx + mapperThreadsCount, mapperThreadsCount, reducerThreadsCount, &partialIndexes, &barrier, &reduceQueue, &reduceMutex};
            pthread_create(&threads[i], nullptr, ReduceThread, (void*)&reducerMemories[idx]);
        }
    }

    // Wait for threads to complete
    for (auto& thread : threads) {
        pthread_join(thread, nullptr);
    }

    // Add partial indexes to the reduction queue
    for (auto& partialIndex : partialIndexes) {
        reduceQueue.push(move(partialIndex));
    }

    // Final index (after reduce phase)
    map<string, vector<int>> finalIndex = move(reduceQueue.front());

    // Print final index
    for (const auto& [word, fileIDs] : finalIndex) {
        cout << word << ": ";
        for (int fileID : fileIDs) {
            cout << fileID << " ";
        }
        cout << endl;
    }

    pthread_barrier_destroy(&barrier);
    pthread_mutex_destroy(&chunkMutex);
    pthread_mutex_destroy(&reduceMutex);
    return 0;
}
