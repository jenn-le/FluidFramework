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
    private readonly deleted = new Set<string>();

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
        if (this.deleted.has(key)) {
            this.deleted.delete(key);
        }

        this.map.set(key, value);
    }

    delete(key: string): boolean {
        this.map.delete(key);
        this.deleted.add(key);
        return true;
    }

    clear(): void {
        for (const key of this.map.keys()) {
            this.delete(key);
        }
    }
}
