#include <iostream>
#include <fstream>
#include <unordered_map>
#include <pthread.h>
#include <string>
#include <vector>
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

    // print the thread buckets with their files
    for (int i = 0; i < mapperThreadsCount; i++)
    {
        cout << "Thread " << i << ":\n";
        for (const string& file : threadBuckets[i].Files)
        {
            cout << file << endl;
        }
        cout << "Total size: " << threadBuckets[i].TotalSize << endl;
        cout << endl;
    }
    return 0;
}