/*!
 * Copyright (c) Microsoft Corporation and contributors. All rights reserved.
 * Licensed under the MIT License.
 */

import { assert } from "@fluidframework/common-utils";

/**
 * An O(1) cache for locally modified (unacked) key/value pairs in SharedPartialMap.
 * It must only be populated with local entries.
 */
export class PendingState<T> {
    private updateNumber = 0;
    private ackedUpdateNumber = 0;
    private readonly pendingKeys: Map<
        string,
        { latestValue?: T; latestUpdateNumber: number; isDeleted: boolean; }> = new Map();
    private pendingClearCount = 0;
    private latestClearUpdateNumber = -1;

    public get size(): number {
        return this.pendingKeys.size;
    }

    get(key: string): { value: T | undefined; keyIsModified: boolean; isDeleted: boolean; } {
        const existing = this.pendingKeys.get(key);
        if (existing !== undefined) {
            assert(
                existing.latestUpdateNumber > this.ackedUpdateNumber,
                "Entry with no pending changes should not be in map.");
            if (existing.latestUpdateNumber > this.latestClearUpdateNumber) {
                return { value: existing.latestValue, keyIsModified: true, isDeleted: existing.isDeleted };
            } else if (this.pendingClearCount > 0) {
                return { value: undefined, keyIsModified: true, isDeleted: true };
            }
        } else if (this.pendingClearCount > 0) {
            return { value: undefined, keyIsModified: true, isDeleted: true };
        }
        return { value: undefined, keyIsModified: false, isDeleted: false };
    }

    set(key: string, value: T) {
        this.updateNumber++;
        const entry = this.pendingKeys.get(key);
        if (entry === undefined) {
            this.pendingKeys.set(key, { latestValue: value, latestUpdateNumber: this.updateNumber, isDeleted: false });
        } else {
            entry.latestValue = value;
            entry.latestUpdateNumber = this.updateNumber;
            entry.isDeleted = false;
        }
    }

    delete(key: string): void {
        this.updateNumber++;
        const entry = this.pendingKeys.get(key);
        if (entry === undefined) {
            this.pendingKeys.set(key, { latestUpdateNumber: this.updateNumber, isDeleted: true });
        } else {
            entry.isDeleted = true;
            entry.latestValue = undefined;
            entry.latestUpdateNumber = this.updateNumber;
        }
    }

    clear(): void {
        this.updateNumber++;
        this.pendingClearCount++;
        this.latestClearUpdateNumber = this.updateNumber;
    }

    ackModify(key: string): void {
        this.ackedUpdateNumber++;
        const existing = this.pendingKeys.get(key);
        assert(existing !== undefined, "Pending key must be in map until acked.");
        if (existing.latestUpdateNumber <= this.ackedUpdateNumber) {
            this.pendingKeys.delete(key);
        }
    }

    ackClear(): void {
        this.ackedUpdateNumber++;
        this.pendingClearCount--;
    }
}
