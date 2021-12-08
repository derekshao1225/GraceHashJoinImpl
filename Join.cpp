#include "Join.hpp"
#include <functional>

/*
 * TODO: Student implementation
 * Input: Disk, Memory, Disk page ids for left relation, Disk page ids for right relation
 * Output: Vector of Buckets of size (MEM_SIZE_IN_PAGE - 1) after partition
 */

void addPage(unsigned int hashOut, Mem *mem, vector<Bucket> *buckVector, Disk *disk, bool left)
{
    for (unsigned int i = 0; i < hashOut; ++i)
    {
        unsigned int mempageSize = mem->mem_page(i)->size();
        if (mempageSize != 0)
        {
            if (left)
            {
                (*buckVector)[i].add_left_rel_page(mem->flushToDisk(disk, i));
            }
            else
            {
                (*buckVector)[i].add_right_rel_page(mem->flushToDisk(disk, i));
            }
        }
    }
}

vector<Bucket> partition(
    Disk *disk,
    Mem *mem,
    pair<unsigned int, unsigned int> left_rel,
    pair<unsigned int, unsigned int> right_rel)
{
    Bucket bucketNew(disk);
    unsigned int memInputId = MEM_SIZE_IN_PAGE - 1;
    unsigned int hashOut = MEM_SIZE_IN_PAGE - 1;
    vector<Bucket> buckVector(hashOut, bucketNew);

    for (unsigned int i = left_rel.first; i < left_rel.second; i++)
    {
        mem->loadFromDisk(disk, i, memInputId);
        Page *bufferIn = mem->mem_page(memInputId);

        for (unsigned int j = 0; j < bufferIn->size(); j++)
        {
            Record currentRecord(bufferIn->get_record(j)); // copy record
            unsigned int bufferID = currentRecord.partition_hash() % hashOut;
            Page *memHash = mem->mem_page(bufferID);
            memHash->loadRecord(currentRecord);

            if (memHash->full())
            {
                buckVector[bufferID].add_left_rel_page(mem->flushToDisk(disk, bufferID));
            }
        }
    }

    addPage(hashOut, mem, &buckVector, disk, true);

    for (unsigned int i = right_rel.first; i < right_rel.second; ++i)
    {
        mem->loadFromDisk(disk, i, memInputId);
        Page *bufferIn = mem->mem_page(memInputId);

        for (unsigned int j = 0; j < bufferIn->size(); j++)
        {
            Record currentRecord(bufferIn->get_record(j));
            unsigned int bufferID = currentRecord.partition_hash() % hashOut;
            Page *memHash = mem->mem_page(bufferID);
            memHash->loadRecord(currentRecord);

            if (memHash->full())
            {
                buckVector[bufferID].add_right_rel_page(mem->flushToDisk(disk, bufferID));
            }
        }
    }

    addPage(hashOut, mem, &buckVector, disk, false);

    mem->reset();
    return buckVector;
}

/*
 * TODO: Student implementation
 * Input: Disk, Memory, Vector of Buckets after partition
 * Output: Vector of disk page ids for join result
 */
vector<unsigned int> probe(Disk *disk, Mem *mem, vector<Bucket> &partitions)
{
    vector<unsigned int> result;
    unsigned int in_id = MEM_SIZE_IN_PAGE - 2;
    unsigned int out_id = MEM_SIZE_IN_PAGE - 1;
    Page *input_page = mem->mem_page(in_id);   // input page ptr
    Page *output_page = mem->mem_page(out_id); // output page ptr
    Page *curr_page = nullptr;
    for (auto bucket : partitions)
    { // get a single bucket: disk_page_ids and disk ptr
        vector<unsigned int> small_rel = bucket.get_left_rel();
        vector<unsigned int> large_rel = bucket.get_right_rel();
        if(bucket.num_right_rel_record < bucket.num_left_rel_record) {
            small_rel = bucket.get_right_rel();
            large_rel = bucket.get_left_rel();
        } 
        for (auto l_id : small_rel)
        {                                         // for each left disk page id
            mem->loadFromDisk(disk, l_id, in_id); // load left disk page to input mem page
            for (unsigned int i = 0; i < input_page->size(); i++)
            { // for each record in input mem page
                Record temp(input_page->get_record(i));
                unsigned int hash_val = temp.probe_hash() % (MEM_SIZE_IN_PAGE - 2); // hash record
                curr_page = mem->mem_page(hash_val);
                curr_page->loadRecord(temp); // add record to hash table in memory
            }
        }
        for (auto r_id : large_rel)
        {
            mem->loadFromDisk(disk, r_id, in_id); // load right disk page to input mem page
            for (unsigned int i = 0; i < input_page->size(); i++)
            { // for each record in input mem page
                Record temp(input_page->get_record(i));
                unsigned int hash_val = temp.probe_hash() % (MEM_SIZE_IN_PAGE - 2); // hash record
                curr_page = mem->mem_page(hash_val);
                for (unsigned int j = 0; j < curr_page->size(); j++)
                { // any left record in this hash page can make pairs
                    Record left = curr_page->get_record(j);
                    if(left == temp) { // equal key is found between this pair
                        output_page->loadPair(left, temp); // add current pair to output mem page
                        if (output_page->full())
                        { // flush output page when it is full
                            result.push_back(mem->flushToDisk(disk, out_id));
                        }
                    }
                }
            }
        }
        for(unsigned int i = 0; i < in_id; i++) { // clear hash table for current bucket
            mem->mem_page(i)->reset();
        }
    }
    if(mem->mem_page(out_id)->size() > 0) { // flush the remaining into disk
        result.push_back(mem->flushToDisk(disk, out_id));
    }
    return result;
}
