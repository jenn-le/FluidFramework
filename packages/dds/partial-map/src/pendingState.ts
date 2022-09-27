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
    private readonly pendingKeys: Map<
        string,
        { count: number; latestValue?: T; latestUpdateNumber: number; isDeleted: boolean; }> = new Map();
    private pendingClearCount = 0;
    private latestClearUpdateNumber = -1;

    get(key: string): { value: T | undefined; keyIsModified: boolean; isDeleted: boolean; } {
        const existing = this.pendingKeys.get(key);
        if (existing !== undefined) {
            assert(existing.count > 0, "Entry with no pending changes should not be in map.");
            if (existing.latestUpdateNumber > this.latestClearUpdateNumber) {
                return { value: existing.latestValue, keyIsModified: true, isDeleted: existing.isDeleted };
            } else if (this.pendingClearCount > 0) {
                return { value: undefined, keyIsModified: true, isDeleted: true };
            }
        }
        return { value: undefined, keyIsModified: false, isDeleted: false };
    }

    set(key: string, value: T) {
        this.updateNumber++;
        let entry = this.pendingKeys.get(key);
        if (entry === undefined) {
            entry = { count: 1, latestValue: value, latestUpdateNumber: this.updateNumber, isDeleted: false };
            this.pendingKeys.set(key, entry);
        } else {
            entry.count++;
            entry.latestValue = value;
            entry.latestUpdateNumber = this.updateNumber;
            entry.isDeleted = false;
        }
    }

    delete(key: string): void {
        this.updateNumber++;
        let entry = this.pendingKeys.get(key);
        if (entry === undefined) {
            entry = { count: 1, latestUpdateNumber: this.updateNumber, isDeleted: true };
        } else {
            entry.count++;
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
        const existing = this.pendingKeys.get(key);
        assert(existing !== undefined, "Pending key must be in map until acked.");
        existing.count--;
        if (existing.count === 0) {
            this.pendingKeys.delete(key);
        }
    }

    ackClear(): void {
        this.pendingClearCount--;
    }
}
