#include <iostream>
#include <fstream>
#include <map>
#include <pthread.h>
#include <string>
#include <vector>
#include <queue>
#include <cctype>
#include <algorithm>
#include <cmath>

using namespace std;

typedef unordered_map<string, vector<int>> ReverseIndex;
typedef pair<string, vector<int>> IndexEntry;

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
    int Start; 
    int End;   
    int FileID;
};

struct MapperThreadMemory
{
    MapperThreadMemory(vector<FileBucket>& fileBuckets
        , queue<Chunk>& chunkQueue, pthread_mutex_t& chunkMutex
        , queue<ReverseIndex>& reduceQueue, pthread_mutex_t& reduceMutex
        , int& mappersDoneCount
        , pthread_mutex_t& mappersCountMutex)
            : FileBuckets(fileBuckets)
            , ChunkQueue(chunkQueue)
            , ReduceQueue(reduceQueue)
            , ChunkMutex(chunkMutex)
            , ReduceMutex(reduceMutex)
            , MappersDoneCount(mappersDoneCount)
            , MappersCountMutex(mappersCountMutex)
    {
    }

    // Dependencies and outputs
    vector<FileBucket>& FileBuckets;
    queue<Chunk>& ChunkQueue;
    queue<ReverseIndex>& ReduceQueue;
    pthread_mutex_t& ChunkMutex;
    pthread_mutex_t& ReduceMutex;

    int& MappersDoneCount;
    pthread_mutex_t& MappersCountMutex;
};

struct ReducerThreadMemory
{
    ReducerThreadMemory(int threadID
        , int mapperThreadsCount
        , int reducerThreadsCount
        , queue<ReverseIndex>& reduceQueue
        , pthread_mutex_t& reduceMutex
        , int& mappersCount
        , pthread_mutex_t& mappersCountMutex
        , pthread_barrier_t& fileOutputBarrier
        , vector<vector<IndexEntry>>& wordBuckets)
            : MapperThreadsCount(mapperThreadsCount)
            , ThreadID(threadID)
            , ReducerThreadsCount(reducerThreadsCount)
            , ReduceQueue(reduceQueue)
            , ReduceMutex(reduceMutex)
            , MappersDoneCount(mappersCount)
            , MappersCountMutex(mappersCountMutex)
            , FileOutputBarrier(fileOutputBarrier)
            , WordBuckets(wordBuckets)
    {
    }

    // Dependencies and outputs
    queue<ReverseIndex>& ReduceQueue;
    pthread_mutex_t& ReduceMutex;

    int ThreadID;
    int ReducerThreadsCount;
    int MapperThreadsCount;
    int& MappersDoneCount;
    pthread_mutex_t& MappersCountMutex;
    pthread_barrier_t& FileOutputBarrier;
    // Printing to files
    vector<vector<IndexEntry>>& WordBuckets;
};

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

void Map(const Chunk& chunk, ReverseIndex& partialIndex)
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

void CombineMaps(ReverseIndex& map1, const ReverseIndex& map2)
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
    MapperThreadMemory& threadMemory = *((MapperThreadMemory*)args);

    while (true)
    {
        Chunk chunk;
        bool hasChunk = false;

        pthread_mutex_lock(&threadMemory.ChunkMutex);
        if (!threadMemory.ChunkQueue.empty())
        {
            chunk = threadMemory.ChunkQueue.front();
            threadMemory.ChunkQueue.pop();
            hasChunk = true;
        }
        pthread_mutex_unlock(&threadMemory.ChunkMutex);

        if (!hasChunk)
        {
            // No more chunks, mark this mapper thread as done
            pthread_mutex_lock(&threadMemory.MappersCountMutex);
            threadMemory.MappersDoneCount++;
            pthread_mutex_unlock(&threadMemory.MappersCountMutex);
            break;
        }

        // Step 2: Process the chunk (no locks needed here)
        ReverseIndex partialIndex;
        Map(chunk, partialIndex);

        // Step 3: Push the partial index to the reduce queue
        pthread_mutex_lock(&threadMemory.ReduceMutex);
        threadMemory.ReduceQueue.push(move(partialIndex));
        pthread_mutex_unlock(&threadMemory.ReduceMutex);
    }

    return NULL;
}


bool CompareByFileIDCount(const IndexEntry& a, const IndexEntry& b)
{
    // Compare first by the count of associated file IDs and then alphabetically
    if (a.second.size() == b.second.size())
        return a.first < b.first;  
    return a.second.size() > b.second.size();
}

void WriteIndexToFiles(ReverseIndex& finalIndex, ReducerThreadMemory& threadMemory)
{
    if (threadMemory.ThreadID == 0)
    {
        // Singlethreaded, iterate through the finalIndex and fill the word buckets
        for (auto& [word, fileIDs] : finalIndex)
        {
            char index = word[0] - 'a';
            threadMemory.WordBuckets[index].push_back({word, fileIDs});
        }
    }

    // Wait until word buckets is actually initialised
    pthread_barrier_wait(&threadMemory.FileOutputBarrier);

    double N = 26.0;
    int start = threadMemory.ThreadID * ceil(N / threadMemory.ReducerThreadsCount);
    int stop = min(N, (threadMemory.ThreadID + 1) * ceil(N / threadMemory.ReducerThreadsCount));

    for (int i = start; i < stop; i++)
    {
        char startingLetter = 'a' + i;
        string fileName(1, startingLetter);
        fileName += ".txt";

        // Sort by the count of associated file IDs and also in alphabetical order
        vector<IndexEntry> sortedWords = threadMemory.WordBuckets[i];
        sort(sortedWords.begin(), sortedWords.end(), CompareByFileIDCount);

        ofstream outFile(fileName);
        for (const auto& [word, fileIDs] : sortedWords)
        {
            outFile << word << ":[";

            // sort the file IDs in ascending order
            auto sortedFileIDs = fileIDs;
            sort(sortedFileIDs.begin(), sortedFileIDs.end());

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
    }
}

void* ReduceThread(void* args)
{
    ReducerThreadMemory& threadMemory = *((ReducerThreadMemory*)args);

    while (true)
    {
        pthread_mutex_lock(&threadMemory.ReduceMutex);
        pthread_mutex_lock(&threadMemory.MappersCountMutex);
        if (threadMemory.ReduceQueue.size() < 2)
        {
            pthread_mutex_unlock(&threadMemory.ReduceMutex);
            if (threadMemory.MapperThreadsCount == threadMemory.MappersDoneCount)
            {
                // Reduce phase is done
                pthread_mutex_unlock(&threadMemory.MappersCountMutex);
                break;
            }

            // Wait until the queue has at least 2 maps
            pthread_mutex_unlock(&threadMemory.MappersCountMutex);
            continue;
        }
        pthread_mutex_unlock(&threadMemory.MappersCountMutex);

        // Get two maps from the queue
        // Use move semantics to avoid copying
        ReverseIndex map1 = move(threadMemory.ReduceQueue.front());
        threadMemory.ReduceQueue.pop();
        ReverseIndex map2 = move(threadMemory.ReduceQueue.front());
        threadMemory.ReduceQueue.pop();
        pthread_mutex_unlock(&threadMemory.ReduceMutex);

        CombineMaps(map1, map2);

        // Push the combined unordered_map back into the queue
        pthread_mutex_lock(&threadMemory.ReduceMutex);
        threadMemory.ReduceQueue.push(move(map1));
        pthread_mutex_unlock(&threadMemory.ReduceMutex);
    }

    // Wait until all reducers are done
    pthread_barrier_wait(&threadMemory.FileOutputBarrier);

    WriteIndexToFiles(threadMemory.ReduceQueue.front(), threadMemory);
    return NULL;
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
        ifstream fileStream(datasetName, ios::ate); // Move the file pointer to the end
        int fileSize = fileStream.tellg();

        fileBuckets.push_back({datasetName, fileSize});
    }

    // Create a queue for file chunks
    // The chunk size is static
    queue<Chunk> chunkQueue;
    int chunkSize = 15000;
    AddChunksToQueue(fileBuckets, chunkSize, chunkQueue);

    // Create a queue for reducing phase
    queue<ReverseIndex> reduceQueue;

    // When all the mappers are done, the reducers can also end
    int mappersDone = 0;
    // Initialize mutexes and the barrier
    pthread_mutex_t chunkMutex;
    pthread_mutex_t reduceMutex;
    pthread_mutex_t mappersDoneMutex;
    pthread_barrier_t fileOutputBarrier;
    pthread_mutex_init(&chunkMutex, nullptr);
    pthread_mutex_init(&reduceMutex, nullptr);
    pthread_mutex_init(&mappersDoneMutex, nullptr);
    pthread_barrier_init(&fileOutputBarrier, nullptr, reducerThreadsCount);

    // Create all threads (both mapping and reducing)
    vector<pthread_t> threads(mapperThreadsCount + reducerThreadsCount);
    vector<vector<IndexEntry>> wordBuckets(26);
    vector<MapperThreadMemory> mapperMemories;
    vector<ReducerThreadMemory> reducerMemories;
    mapperMemories.reserve(mapperThreadsCount);
    reducerMemories.reserve(reducerThreadsCount);

    for (int threadID = 0; threadID < mapperThreadsCount + reducerThreadsCount; threadID++)
    {
        if (threadID < mapperThreadsCount)
        {
            mapperMemories.emplace_back(
                fileBuckets, chunkQueue, chunkMutex, reduceQueue, reduceMutex, mappersDone, mappersDoneMutex
            );

            pthread_create(&threads[threadID], nullptr, MapThread, &mapperMemories.back());
        }
        else
        {
            reducerMemories.emplace_back(
                threadID - mapperThreadsCount, mapperThreadsCount, reducerThreadsCount, reduceQueue, reduceMutex, mappersDone, mappersDoneMutex, fileOutputBarrier, wordBuckets
            );

            pthread_create(&threads[threadID], nullptr, ReduceThread, &reducerMemories.back());
        }
    }

    // Wait for threads to complete
    for (auto& thread : threads)
    {
        pthread_join(thread, nullptr);
    }

    pthread_mutex_destroy(&chunkMutex);
    pthread_mutex_destroy(&reduceMutex);
    pthread_mutex_destroy(&mappersDoneMutex);
    pthread_barrier_destroy(&fileOutputBarrier);
    return 0;
}
