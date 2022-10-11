/*!
 * Copyright (c) Microsoft Corporation and contributors. All rights reserved.
 * Licensed under the MIT License.
 */

/**
 * An O(1) cache for recently accessed key/value pairs in SharedPartialMap.
 * It must only be populated with acked entries.
 */
export class SequencedState<T> {
    // Contains reads and sequenced writes. Read (un-modified) values can be evicted at any time, and mutated values
    // can be evicted after those values have been persisted via flush.
    private readonly allEntries = new Map<string, T>();

    // A list of all mutations (sets/deletes) since the last flush.
    private readonly operations: [sequenceNumber: number, key: string, value?: T][] = [];

    // A set of all mutated (set/deleted) keys since the last flush.
    private readonly modified = new Set<string>();

    constructor(
        private cacheSizeHint: number,
        private readonly onEvict: (evictionCountHint: number) => void) { }

    public get unflushedChangeCount(): number {
        return this.modified.size;
    }

    public get size(): number {
        return this.allEntries.size;
    }

    public setCacheSizeHint(cacheSizeHint: number) {
        this.cacheSizeHint = cacheSizeHint;
    }

    public cache(key: string, value: T): void {
        this.allEntries.set(key, value);
        this.evict();
    }

    public get(key: string): { value: T | undefined; keyIsModified: boolean; isDeleted: boolean; } {
        const existing = this.allEntries.get(key);
        const isDeleted = existing === undefined && this.modified.has(key) && !this.allEntries.has(key);
        return { value: existing, keyIsModified: existing !== undefined || isDeleted, isDeleted };
    }

    public set(key: string, value: T, sequenceNumber: number): void {
        this.modified.add(key);
        this.operations.push([sequenceNumber, key, value]);
        this.allEntries.set(key, value);
        this.evict();
    }

    public delete(key: string, sequenceNumber: number): void {
        this.modified.add(key);
        this.operations.push([sequenceNumber, key]);
        this.allEntries.delete(key);
        this.evict();
    }

    public clear(): void {
        this.operations.splice(0);
        this.modified.clear();
        this.allEntries.clear();
    }

    public getFlushableChanges(): [updates: Map<string, T>, deletes: Set<string>] {
        const updates = new Map<string, T>();
        const deletes = new Set<string>();
        for (const mutation of this.operations) {
            const [_, key, value] = mutation;
            if (mutation.length === 3) {
                deletes.delete(key);
                updates.set(key, value as T);
            } else {
                deletes.add(key);
                updates.delete(key);
            }
        }
        return [updates, deletes];
    }

    public flush(referenceSequenceNumber: number): void {
        if (this.operations.length === 0) {
            return;
        }
        this.modified.clear();
        let opIndex = 0;
        while (opIndex < this.operations.length) {
            const [sequenceNumber] = this.operations[opIndex];
            if (sequenceNumber > referenceSequenceNumber) {
                break;
            }
            opIndex++;
        }
        this.operations.splice(0, opIndex);
        for (const [_, key] of this.operations) {
            this.modified.add(key);
        }
    }

    public evict(workingSetSizeOverride?: number): void {
        const workingSetSize = Math.max(this.allEntries.size, workingSetSizeOverride ?? 0);
        if (workingSetSize > this.cacheSizeHint) {
            const evictableCount = workingSetSize - this.unflushedChangeCount;
            if (evictableCount > this.cacheSizeHint / 2) {
                let toEvict = this.cacheSizeHint / 2;
                this.onEvict(toEvict);
                for (const key of this.allEntries.keys()) {
                    if (!this.modified.has(key)) {
                        this.allEntries.delete(key);
                        toEvict--;
                    }
                    if (toEvict === 0) {
                        break;
                    }
                }
            }
        }
    }
}
