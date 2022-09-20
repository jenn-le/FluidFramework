/*!
 * Copyright (c) Microsoft Corporation and contributors. All rights reserved.
 * Licensed under the MIT License.
 */

import { notStrictEqual } from "assert";
import { Serializable } from "@fluidframework/datastore-definitions";
import { IHashbrown } from "./interfaces";
import { tombstone, Tombstone } from "./common";

export class Hashbrown<T = Serializable> implements IHashbrown<T> {
    private readonly map = new Map<string, T | Tombstone>();

    constructor(initialValues?: [string, T | Tombstone][]) {
        if (initialValues !== undefined) {
            for (const [key, value] of initialValues) {
                this.map.set(key, value);
            }
        }
    }

    get(key: string): T | Tombstone | undefined {
        return this.map.get(key);
    }

    set(key: string, value: T) {
        notStrictEqual(value, tombstone);

        // If the value is undefined, we mark the key for deletion
        this.map.set(key, value === undefined ? tombstone : value);
    }

    delete(key: string): boolean {
        this.map.set(key, tombstone);
        return true;
    }
}
