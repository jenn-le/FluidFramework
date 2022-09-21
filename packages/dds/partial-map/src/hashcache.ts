/*!
 * Copyright (c) Microsoft Corporation and contributors. All rights reserved.
 * Licensed under the MIT License.
 */

import { Serializable } from "@fluidframework/datastore-definitions";
import { IHashcache } from "./interfaces";

/**
 * TODO:
 * - cache eviction
 */
export class Hashcache<T = Serializable> implements IHashcache<T> {
    private readonly map = new Map<string, T>();
    private updates = new Map<string, T>();
    private deletes = new Set<string>();

    constructor(initialValues?: [string, T][]) {
        if (initialValues !== undefined) {
            for (const [key, value] of initialValues) {
                this.map.set(key, value);
            }
        }
    }

    get(key: string): T | undefined {
        return this.map.get(key);
    }

    has(key: string): boolean {
        return this.map.has(key);
    }

    set(key: string, value: T) {
        if (this.deletes.has(key)) {
            this.deletes.delete(key);
        }

        this.updates.set(key, value);
        this.map.set(key, value);
    }

    delete(key: string): boolean {
        this.map.delete(key);
        this.deletes.add(key);
        return true;
    }

    flushUpdates(): [Map<string, T>, Set<string>] {
        const [updates, deletes] = [this.updates, this.deletes];
        this.updates = new Map<string, T>();
        this.deletes = new Set<string>();

        return [updates, deletes];
    }

    clear(): void {
        for (const key of this.map.keys()) {
            this.delete(key);
        }
    }
}
