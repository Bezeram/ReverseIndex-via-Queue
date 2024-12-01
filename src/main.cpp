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
struct FileBucket
{
    string File;
    int Size;
};

// Represents a chunk of a file (portion to be processed)
struct Chunk
{
    string File;
    int Start;  // Start byte
    int End;    // End byte
    int FileID; // Unique ID for the file
};

// Thread memory for mapping phase
struct MapperThreadMemory
{
    int ThreadID;
    int MapperThreadsCount;
    vector<FileBucket>* FileBuckets;
    vector<unordered_map<string, vector<int>>>* PartialIndexes;
    pthread_barrier_t* MapperReducerBarrier;
    queue<Chunk>* ChunkQueue;
    pthread_mutex_t* ChunkMutex;
    queue<unordered_map<string, vector<int>>>* ReduceQueue;
    pthread_mutex_t* ReduceMutex; // For safely pushing into the reduce queue
};

// Thread memory for reducing phase
struct ReducerThreadMemory
{
    int ThreadID;
    int MapperThreadsCount;
    int ReducerThreadsCount;
    vector<unordered_map<string, vector<int>>>* PartialIndexes;
    pthread_barrier_t* MapperReducerBarrier;
    queue<unordered_map<string, vector<int>>>* ReduceQueue;
    pthread_mutex_t* ReduceMutex;
};

// Adds file chunks to the queue for processing
void AddChunksToQueue(const vector<FileBucket>& fileBuckets, int chunkSize, queue<Chunk>& chunkQueue)
{
    for (int fileID = 0; fileID < fileBuckets.size(); fileID++)
    {
        const auto& file = fileBuckets[fileID];
        ifstream fin(file.File);

        if (!fin.is_open())
        {
            cerr << "Error opening file: " << file.File << endl;
            continue;
        }

        int start = 0;
        while (start < file.Size)
        {
            int end = min(start + chunkSize, file.Size);
            fin.seekg(end);

            // Move 'end' to the next space or newline to avoid splitting words
            char c;
            while (end < file.Size && fin.get(c) && !isspace(c))
            {
                end++;
            }

            // Add the chunk to the queue
            chunkQueue.push({file.File, start, end, fileID + 1});
            start = end;
        }
    }
}

void Map(Chunk chunk, unordered_map<string, vector<int>>& partialIndex)
{
    ifstream fin(chunk.File);
    fin.seekg(chunk.Start);
    string word;

    int position = chunk.Start;
    while (position < chunk.End && !fin.eof())
    {
        fin >> word;

        // Make the word lowercase and keep only alpha characters
        for (int i = 0; i < word.size(); )
        {
            if (!isalpha(word[i]))
            {
                word.erase(i, 1);
            }
            else
            {
                word[i] = tolower(word[i]);
                i++;
            }
        }

        // Update the index
        if (!word.empty())
        {
            if (find(partialIndex[word].begin(), partialIndex[word].end(), chunk.FileID) == partialIndex[word].end())
            {
                partialIndex[word].push_back(chunk.FileID); // Record the file ID
            }
        }

        position = fin.tellg();
    }
}

void CombineMaps(unordered_map<string, vector<int>>& map1, const unordered_map<string, vector<int>>& map2)
{
    for (const auto& [word, fileIDs] : map2)
    {
        vector<int>& combinedFileIDs = map1[word];
        for (int fileID : fileIDs)
        {
            if (find(combinedFileIDs.begin(), combinedFileIDs.end(), fileID) == combinedFileIDs.end())
            {
                combinedFileIDs.push_back(fileID);
            }
        }
    }
}

void* MapThread(void* args)
{
    MapperThreadMemory* threadMemory = (MapperThreadMemory*)args;

    while (true)
    {
        pthread_mutex_lock(threadMemory->ChunkMutex);
        if (threadMemory->ChunkQueue->empty())
        {
            pthread_mutex_unlock(threadMemory->ChunkMutex);
            break; // No more chunks
        }
        Chunk chunk = threadMemory->ChunkQueue->front();
        threadMemory->ChunkQueue->pop();
        pthread_mutex_unlock(threadMemory->ChunkMutex);

        // Map the chunk
        Map(chunk, (*threadMemory->PartialIndexes)[threadMemory->ThreadID]);
    }

    // Push the thread's partial index to the reduceQueue
    pthread_mutex_lock(threadMemory->ReduceMutex);
    threadMemory->ReduceQueue->push(move((*threadMemory->PartialIndexes)[threadMemory->ThreadID]));
    pthread_mutex_unlock(threadMemory->ReduceMutex);

    // Signal completion
    pthread_barrier_wait(threadMemory->MapperReducerBarrier);
    return NULL;
}

// Reducer thread function
void* ReduceThread(void* args)
{
    ReducerThreadMemory* threadMemory = (ReducerThreadMemory*)args;

    // Wait until mappers are done
    pthread_barrier_wait(threadMemory->MapperReducerBarrier);

    while (true)
    {
        unordered_map<string, vector<int>> map1, map2;

        pthread_mutex_lock(threadMemory->ReduceMutex);
        if (threadMemory->ReduceQueue->size() < 2)
        {
            pthread_mutex_unlock(threadMemory->ReduceMutex);
            break; // Not enough maps to process
        }

        // Get two maps from the queue
        map1 = move(threadMemory->ReduceQueue->front());
        threadMemory->ReduceQueue->pop();
        map2 = move(threadMemory->ReduceQueue->front());
        threadMemory->ReduceQueue->pop();
        pthread_mutex_unlock(threadMemory->ReduceMutex);

        CombineMaps(map1, map2);

        // Push the combined unordered_map back into the queue
        pthread_mutex_lock(threadMemory->ReduceMutex);
        threadMemory->ReduceQueue->push(move(map1));
        pthread_mutex_unlock(threadMemory->ReduceMutex);
    }

    return NULL;
}

bool CompareByFileIDCount(const pair<string, vector<int>>& a, const pair<string, vector<int>>& b)
{
    if (a.second.size() == b.second.size())
    {
        return a.first < b.first;  // Compare based on word in ascending order
    }
    return a.second.size() > b.second.size();  // Compare based on size of vector<int> in descending order
}

void WriteIndexToFiles(const unordered_map<string, vector<int>>& finalIndex)
{
    unordered_map<char, ofstream> fileStreams;
    // open all files in out mode
    for (int c = 'a'; c <= 'z'; c++)
    {
        string fileName(1, c);
        fileName += ".txt";
        fileStreams[c].open(fileName);
    }

    // Collect the entries from the finalIndex unordered_map into a vector of pairs
    vector<pair<string, vector<int>>> sortedIndex(finalIndex.begin(), finalIndex.end());

    // Sort the vector based on the size of the fileID list in descending order
    sort(sortedIndex.begin(), sortedIndex.end(), CompareByFileIDCount);

    // Write the sorted entries to the output files
    for (const auto& [word, fileIDs] : sortedIndex)
    {
        if (word.empty())
        {
            continue;
        }

        // Determine the file name based on the starting letter
        char startingLetter = tolower(word[0]);
        string fileName(1, startingLetter);
        fileName += ".txt";

        // sort the file IDs
        auto sortedFileIDs = fileIDs;
        sort(sortedFileIDs.begin(), sortedFileIDs.end());

        ofstream& outFile = fileStreams[startingLetter];
        outFile << word << ":[";

        // Write the sorted file IDs
        for (size_t i = 0; i < sortedFileIDs.size(); i++)
        {
            outFile << sortedFileIDs[i];
            if (i < sortedFileIDs.size() - 1)
            {
                outFile << " ";
            }
        }
        outFile << "]\n";
    }

    // Close all file streams
    for (auto& [_, stream] : fileStreams)
    {
        stream.close();
    }
}

int main(int argc, char** argv)
{
    if (argc != 4)
    {
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
    for (int i = 0; i < n; i++)
    {
        string datasetName;
        fin >> datasetName;

        // Get file size
        ifstream fileStream(datasetName, ios::binary | ios::ate);
        int fileSize = fileStream.tellg();

        fileBuckets.push_back({datasetName, fileSize});
    }

    // Create a queue for file chunks
    queue<Chunk> chunkQueue;
    int chunkSize = 15000; // Define a chunk size (cannot be smaller than the biggest word in any file)
    AddChunksToQueue(fileBuckets, chunkSize, chunkQueue);

    // Initialize partial indexes for threads
    vector<unordered_map<string, vector<int>>> partialIndexes(mapperThreadsCount);

    // Create and initialize barrier for synchronizing mapping and reducing phases
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, nullptr, mapperThreadsCount + reducerThreadsCount);

    // Create a queue for reducing phase
    queue<unordered_map<string, vector<int>>> reduceQueue;

    // Initialize mutexes
    pthread_mutex_t chunkMutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t reduceMutex = PTHREAD_MUTEX_INITIALIZER;

    // Create all threads (both mapping and reducing)
    vector<pthread_t> threads(mapperThreadsCount + reducerThreadsCount);
    vector<MapperThreadMemory> mapperMemories(mapperThreadsCount);
    vector<ReducerThreadMemory> reducerMemories(reducerThreadsCount);

    for (int i = 0; i < mapperThreadsCount + reducerThreadsCount; i++)
    {
        if (i < mapperThreadsCount)
        {
            mapperMemories[i] = {
                i, mapperThreadsCount, &fileBuckets, &partialIndexes, &barrier,
                &chunkQueue, &chunkMutex, &reduceQueue, &reduceMutex};
            pthread_create(&threads[i], nullptr, MapThread, (void*)&mapperMemories[i]);
        }
        else
        {
            int idx = i - mapperThreadsCount;
            reducerMemories[idx] = {
                idx + mapperThreadsCount, mapperThreadsCount, reducerThreadsCount, 
                &partialIndexes, &barrier, &reduceQueue, &reduceMutex};
            pthread_create(&threads[i], nullptr, ReduceThread, (void*)&reducerMemories[idx]);
        }
    }

    // Wait for threads to complete
    for (auto& thread : threads)
    {
        pthread_join(thread, nullptr);
    }

    // Final index (after reduce phase)
    unordered_map<string, vector<int>> finalIndex = move(reduceQueue.front());

    WriteIndexToFiles(finalIndex);

    pthread_barrier_destroy(&barrier);
    pthread_mutex_destroy(&chunkMutex);
    pthread_mutex_destroy(&reduceMutex);
    return 0;
}
