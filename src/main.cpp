#include <iostream>
#include <fstream>
#include <map>
#include <pthread.h>
#include <string>
#include <vector>
#include <queue>
#include <mutex>
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
    int Start; // Start byte
    int End;   // End byte
    int FileID; // Unique ID for the file
};

// Global queue of chunks and mutex for synchronization
queue<Chunk> chunkQueue;
mutex chunkMutex;

// Reads dataset file names and sizes from the input file
vector<FileBucket> ReadDatasetNames(const string& inputPath) {
    ifstream fin(inputPath);
    vector<FileBucket> datasetNames;

    int n;
    fin >> n;
    for (int i = 0; i < n; i++) {
        string datasetName;
        fin >> datasetName;

        // Get file size
        ifstream fileStream(datasetName, ios::binary | ios::ate);
        int fileSize = fileStream.tellg();

        datasetNames.push_back({datasetName, fileSize});
    }
    return datasetNames;
}

// Thread memory for mapping phase
struct ThreadMemory {
    int ThreadID;
    vector<map<string, vector<int>>>* PartialIndexes; // Updated for file IDs
    pthread_barrier_t* MapperBarrier;
};

// Adds file chunks to the global queue for processing
void AddChunksToQueue(const vector<FileBucket>& fileBuckets, int chunkSize) {
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

// Worker thread function
void* WorkerThread(void* args) {
    ThreadMemory* threadMemory = (ThreadMemory*)args;

    // Mapping phase
    while (true) {
        chunkMutex.lock();
        if (chunkQueue.empty()) {
            chunkMutex.unlock();
            break; // No more chunks
        }
        Chunk chunk = chunkQueue.front();
        chunkQueue.pop();
        chunkMutex.unlock();

        Map(chunk, (*threadMemory->PartialIndexes)[threadMemory->ThreadID]);
    }

    pthread_barrier_wait(threadMemory->MapperBarrier);

    // TODO: Implement dynamic Reduce phase
    return NULL;
}

// Reduces partial indexes into a final index
void Reduce(vector<map<string, vector<int>>>& partialIndexes, map<string, vector<int>>& finalIndex) {
    for (const auto& partialIndex : partialIndexes) {
        for (const auto& [word, fileIDs] : partialIndex) {
            vector<int>& finalFileIDs = finalIndex[word];
            for (int fileID : fileIDs) {
                if (find(finalFileIDs.begin(), finalFileIDs.end(), fileID) == finalFileIDs.end()) {
                    finalFileIDs.push_back(fileID); // Merge unique file IDs
                }
            }
        }
    }
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
    vector<FileBucket> fileBuckets = ReadDatasetNames(inputFile);

    // Add chunks to the global queue
    const int chunkSize = 1024; // Define a chunk size
    AddChunksToQueue(fileBuckets, chunkSize);

    // Initialize partial indexes for threads
    vector<map<string, vector<int>>> partialIndexes(mapperThreadsCount);

    // Create and initialize mapper threads
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, nullptr, mapperThreadsCount);

    vector<pthread_t> threads(mapperThreadsCount);
    vector<ThreadMemory> threadMemories(mapperThreadsCount);

    for (int i = 0; i < mapperThreadsCount; i++) {
        threadMemories[i] = {i, &partialIndexes, &barrier};
        pthread_create(&threads[i], nullptr, WorkerThread, (void*)&threadMemories[i]);
    }

    // Wait for threads to complete
    for (auto& thread : threads) {
        pthread_join(thread, nullptr);
    }

    // Reduce phase
    map<string, vector<int>> finalIndex;
    Reduce(partialIndexes, finalIndex);

    // Print final index
    for (const auto& [word, fileIDs] : finalIndex) {
        cout << word << ": ";
        for (int fileID : fileIDs) {
            cout << fileID << " ";
        }
        cout << endl;
    }

    pthread_barrier_destroy(&barrier);
    return 0;
}
