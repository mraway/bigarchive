#include "dedup_types.hpp"
typedef struct _node {
    struct _node* next;
    struct _node* prev;
    struct _node* next_hash;
    const Block* blk;
} LruNode;

#define LRU_HASH_SIZE 4096

class LruCache
{
    public:
        LruCache(int size);

        void AddItems(const Segment& seg);
        bool SearchItem(const Block& blk);
    private:
        void InsertHash(LruNode *n);
        void RemoveHash(LruNode *n);
        void AddItem(const Block& blk);
        void DropItem();
        int curSize;
        int maxSize;
        LruNode *head;
        LruNode *tail;
        LruNode *hash[LRU_HASH_SIZE];
};
