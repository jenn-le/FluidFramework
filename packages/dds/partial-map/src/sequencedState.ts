/*!
 * Copyright (c) Microsoft Corporation and contributors. All rights reserved.
 * Licensed under the MIT License.
 */

import { assert } from "console";

/**
 * An O(1) cache for recently accessed key/value pairs in SharedPartialMap.
 * It must only be populated with acked entries.
 */
export class SequencedState<T> {
    private readonly map = new Map<string, T>();
    private updates = new Map<string, T>();
    private deletes = new Set<string>();
    private pendingFlushCount = 0;

    constructor(private readonly cacheSizeHint: number) { }

    public get unflushedChangeCount(): number {
        return this.updates.size + this.deletes.size;
    }

    public cache(key: string, value: T): void {
        this.map.set(key, value);
        this.evict();
    }

    public get(key: string): { value: T | undefined; keyIsModified: boolean; isDeleted: boolean; } {
        const existing = this.map.get(key);
        const isDeleted = existing === undefined && this.deletes.has(key);
        return { value: existing, keyIsModified: existing !== undefined || isDeleted, isDeleted };
    }

    public set(key: string, value: T): void {
        if (this.deletes.has(key)) {
            this.deletes.delete(key);
        }

        this.updates.set(key, value);
        this.map.set(key, value);
        this.evict();
    }

    public delete(key: string): void {
        this.map.delete(key);
        this.deletes.add(key);
    }

    public startFlush(): [Map<string, T>, Set<string>] {
        assert(this.pendingFlushCount === 0, "Reentrant flush.");
        this.pendingFlushCount++;
        const [updates, deletes] = [this.updates, this.deletes];
        this.updates = new Map<string, T>();
        this.deletes = new Set<string>();
        return [updates, deletes];
    }

    public endFlush(): void {
        this.pendingFlushCount--;
    }

    private evict(): void {
        if (this.pendingFlushCount <= 0
            && this.map.size > this.cacheSizeHint
            && this.unflushedChangeCount < this.map.size * 2) {
            let toEvict = this.map.size - this.cacheSizeHint;
            for (const key of this.map.keys()) {
                if (!this.deletes.has(key) && !this.updates.has(key)) {
                    this.map.delete(key);
                    toEvict--;
                }
                if (toEvict === 0) {
                    break;
                }
            }
        }
    }
}
