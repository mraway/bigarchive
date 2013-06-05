#include "lru_cache.hpp"
#include "dedup_types.hpp"


LruCache::LruCache(int size)
{
    curSize = 0;
    maxSize = size;
    head = NULL;
    tail = NULL;
    memset(hash, 0, sizeof(hash));
}

void LruCache::AddItems(const Segment& seg)
{
    for (uint32_t i = 0; i < seg.blocklist_.size(); i ++) {
        if (!SearchItem(seg.blocklist_[i]))
        {
            if (curSize >= maxSize)
            {
                DropItem();
            }
            else
                curSize++;
            AddItem(seg.blocklist_[i]);
        }
            int count = 0;
            for(LruNode *q = head; q != NULL; q = q->next)
                count++;
    }
}

void LruCache::AddItem(const Block& blk)
{
    LruNode *p;
    if (head == NULL)
    {
        p = new LruNode();
        p->blk = &blk;
        p->next = NULL;
        p->prev = NULL;
        InsertHash(p);
        head = p;
        tail = head;
    }
    else
    {
        p = new LruNode();
        p->blk = &blk;
        p->next = NULL;
        p->prev = tail;
        InsertHash(p);
        tail->next = p;
        tail = p;
    }
}

void LruCache::DropItem()
{
    LruNode *p = head;
    RemoveHash(p);
    head = head->next;
    delete p;
}

bool LruCache::SearchItem(const Block& blk)
{
    if (head == NULL)
    {
        return false;
    }
    else
    {
        int i = ((uint16_t*)blk.cksum_)[0] % LRU_HASH_SIZE;
        LruNode *p = hash[i];
        while(p != NULL)
        {
            if (*(p->blk) == blk)
            {
                if (p == tail)
                    return true;
                else if (p == head)
                {
                    head->next->prev = NULL;
                    head = head->next;
                    p->prev = tail;
                    p->next = NULL;
                    tail->next = p;
                    tail = p;
                }
                else
                {
                    p->prev->next = p->next;
                    p->next->prev = p->prev;
                    p->prev = tail;
                    p->next = NULL;
                    tail->next = p;
                    tail = p;
                }
                return true;
            }
            p = p->next_hash;
        }
        return false;
    }
}
void LruCache::InsertHash(LruNode *n)
{
    int i = ((uint16_t*)n->blk->cksum_)[0] % LRU_HASH_SIZE;
    n->next_hash = hash[i];
    hash[i] = n;
}

void LruCache::RemoveHash(LruNode *n)
{
    int i = ((uint16_t*)n->blk->cksum_)[0] % LRU_HASH_SIZE;
    LruNode *p = hash[i];
    if (p == NULL)
        return;
    else if (*(p->blk) == *(n->blk))
    {
        hash[i] = p->next_hash;
    }
    else
    {
        while(p->next_hash != NULL)
        {
            if (*(p->next_hash->blk) == *(n->blk))
            {
                LruNode *t = p->next_hash;
                p->next_hash = t->next_hash;
                return;
            }
            p = p->next_hash;
        }
    }
}
