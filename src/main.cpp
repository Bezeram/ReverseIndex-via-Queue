#include <iostream>
#include <fstream>
#include <map>
#include <pthread.h>
#include <string>
#include <vector>
#include <cctype>
#include <algorithm>

using namespace std;

struct FileBucket
{
    string File;
    int Size;
};

vector<FileBucket> ReadDatasetNames(const string& inputPath)
{
    ifstream fin(inputPath);

    vector<FileBucket> datasetNames;
    int n;
    fin >> n;
    for (int i = 0; i < n; i++)
    {
        string datasetName;
        fin >> datasetName;

        // Get file size
        ifstream fin(datasetName);
        fin.seekg(0, ios::end);
        int fileSize = fin.tellg();

        datasetNames.push_back({ datasetName, fileSize });
    }

    return datasetNames;
}

struct ThreadBucket
{
    vector<string> Files;
    int TotalSize = 0;
};

typedef map<string, int> InverseIndexMap;

struct ThreadMemory
{
    ThreadMemory(string file, int mapID, int reduceIDStart, int reduceIDStop, vector<InverseIndexMap>& inverseIndexes, pthread_barrier_t& mapperBarrier)
        : File(file), MapID(mapID), ReduceIDStart(reduceIDStart), ReduceIDStop(reduceIDStop), InverseIndexes(inverseIndexes), MapperBarrier(mapperBarrier) {}
    string File;
    int MapID;
    int ReduceIDStart, ReduceIDStop;

    vector<InverseIndexMap>& InverseIndexes;
    pthread_barrier_t& MapperBarrier;
};

void AssignFilesToThreads(const vector<FileBucket>& fileBuckets, int mapperThreadsCount, vector<ThreadBucket>& threadBuckets)
{
    // Calculate average file size
    float averageFilesize = 0;
    for (const FileBucket& d : fileBuckets)
    {
        averageFilesize += d.Size;
    }
    averageFilesize /= fileBuckets.size();

    // Sort files by size
    vector<FileBucket> sortedFileBuckets = fileBuckets;
    std::sort(sortedFileBuckets.begin(), sortedFileBuckets.end(), [](const FileBucket& a, const FileBucket& b) {
        return a.Size < b.Size;
    });

    int idxThread = 0;
    int idxFile = 0;
    while (idxFile < sortedFileBuckets.size())
    {
        if (threadBuckets[idxThread].TotalSize + sortedFileBuckets[idxFile].Size <= averageFilesize)
        {
            threadBuckets[idxThread].Files.push_back(sortedFileBuckets[idxFile].File);
            threadBuckets[idxThread].TotalSize += sortedFileBuckets[idxFile].Size;
            idxFile++;
        }
        idxThread = (idxThread + 1) % mapperThreadsCount;

        // If the current file bucket is larger than the average size,
        // just add the rest of the buckets to the threads evenly
        if (sortedFileBuckets[idxFile].Size > averageFilesize)
            break;
    }
    // Assign the rest of the files
    while (idxFile < sortedFileBuckets.size())
    {
        threadBuckets[idxThread].Files.push_back(sortedFileBuckets[idxFile].File);
        threadBuckets[idxThread].TotalSize += sortedFileBuckets[idxFile].Size;
        idxFile++;
        idxThread = (idxThread + 1) % mapperThreadsCount;
    }
}

void Map(const string& file, ThreadMemory* input)
{
    ifstream fin(input->File);
    InverseIndexMap& threadMap = (*input->InverseIndexes)[input->MapID];

    while (!fin.eof())
    {
        string word;
        fin >> word;
        // Erase everything which isn't alphabet letters
        for (int i = 0; i < word.size();)
            if (!isalpha(word[i]))
                word.erase(word.begin() + i);
            else
            {
                word[i] = tolower(word[i]); 
                i++;
            }

        threadMap[word] = input->MapID;
    }
}

void Reduce(const ThreadMemory& input)
{
    // TODO    
}

void* WorkerThread(void* args)
{
    ThreadMemory* input = (ThreadMemory*)args;
    
    Map(input->File, input);

    // Wait for all of the mappers to finish
    pthread_barrier_wait(&input->MapperBarrier);

    return NULL;
}

void printThreadBuckets(const vector<ThreadBucket>& threadBuckets)
{
    for (int i = 0; i < threadBuckets.size(); i++)
    {
        cout << "Thread " << i << ":\n";
        for (const string& file : threadBuckets[i].Files)
        {
            cout << file << endl;
        }
        cout << "Total size: " << threadBuckets[i].TotalSize << endl;
        cout << endl;
    }
}
void printInverseIndexes(const vector<ThreadMemory>& inverseIndexThreads)
{
    for (const auto& threadResult : inverseIndexThreads)
    {
        cout << "Thread " << threadResult.File << endl;
        for (const auto& [key, value] : threadResult.InverseIndex)
            cout << key << " " << value << endl;
        cout << endl;
    }
}

int main(int argc, char **argv)
{
    if (argc != 4)
    {
        cerr << "Usage: " << argv[0] << " <no_mapper_threads> <no_reducer_threads> <input_file>" << endl;
        return 1;
    }

    int mapperThreadsCount = stoi(argv[1]);
    int reducerThreadsCount = stoi(argv[2]);
    string inputFile = argv[3];

    vector<FileBucket> fileBuckets = ReadDatasetNames(inputFile);

    vector<ThreadBucket> threadBuckets(mapperThreadsCount);
    AssignFilesToThreads(fileBuckets, mapperThreadsCount, threadBuckets);

    int noThreads = fileBuckets.size();
    vector<pthread_t> threads(noThreads);
    vector<InverseIndexMap> inverseIndexMaps(noThreads);
    vector<ThreadMemory> inverseIndexThreads(noThreads);
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, NULL, mapperThreadsCount);

    int threadID = 0;
    for (const ThreadBucket& tb : threadBuckets)
    {
        for (const string& file : tb.Files)
        {
            inverseIndexThreads[threadID].File = file;
            inverseIndexThreads[threadID].MapID = threadID; // Each thread has their own unique fileX
            inverseIndexThreads[threadID].MapID = threadID; // Each thread has their own unique fileX
            inverseIndexThreads[threadID].MapperBarrier = barrier;
            pthread_create(&threads[threadID], NULL, WorkerThread, (void*)&inverseIndexThreads[threadID]);
            threadID++;
        }
    }

    for (int i = 0; i < threads.size(); i++)
    {
        void* status;
        int r = pthread_join(threads[i], &status);
 
        if (r) {
            printf("Eroare la asteptarea thread-ului %d\n", i);
            exit(-1);
        }
    }

    // print inverse indexes
    printInverseIndexes(inverseIndexThreads);
    
    return 0;
}