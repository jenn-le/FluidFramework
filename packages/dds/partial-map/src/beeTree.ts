/* eslint-disable no-bitwise */
/*!
 * Copyright (c) Microsoft Corporation and contributors. All rights reserved.
 * Licensed under the MIT License.
 */

import { IFluidHandle } from "@fluidframework/core-interfaces";
import { Serializable } from "@fluidframework/datastore-definitions";
import { IBeeTree, IHandleProvider, IQueenBee } from "./interfaces";

export class BeeTree<T = any> implements IBeeTree<T>, IHandleProvider {
    private readonly map = new Map<string, T>();

	constructor(node: IQueenBee) { }

	async get(key: string): Promise<T | undefined> {
        return this.map.get(key);
	}

	async batchUpdate(updates: Map<string, T>, deletes: Set<string>): Promise<string[]> {
        const blobsToGc: string[] = [];

		for (const [key, value] of updates.entries()) {
            if (this.map.set(key, value)) {
                blobsToGc.push(key);
            } else {
                return blobsToGc;
            }
        }

        for (const key of deletes.keys()) {
            if (this.map.delete(key)) {
                blobsToGc.push(key);
            } else {
                return blobsToGc;
            }
        }

        return [];
	}

    getGcData(): IFluidHandle[] {
        throw new Error("Method not implemented.");
    }
}

// A node in a BeeTree

interface IBeeTreeNode<T> {
    get(key: string): T;
    insert(key: string, value: T): void; // TODO return bool or something?
}

class BTreeNode<T> implements IBeeTreeNode<T> {
    public constructor(public readonly keys: readonly string[], public readonly children: readonly IBeeTreeNode<T>[]) {

    }

    public get(key: string): T {
        for (let i = 0; i < this.keys.length; i++) {
            if (key < this.keys[i]) {
                return this.children[i].get(key);
            }
        }

        return this.children[this.children.length - 1].get(key);
    }


    public insert(key: string, value: T): void {
        throw new Error("Method not implemented.");
    }
}

/**
* The value xor'd with the result index when a search fails.
*/
const failureXor = -1;

/**
* Performs a binary search on the sorted array.
* @returns the index of the key for `search`, or (if not present) the index it would have been inserted into xor'd
* with `failureXor`. Note that negating is not an adequate solution as that could result in -0.
*/
function search<T>(
   elements: readonly T[],
   target: T,
   comparator: (a: T, b: T) => number,
): number | undefined {
   let low = 0;
   let high = elements.length - 1;
   let mid = high >> 1;
   while (low < high) {
       const c = comparator(target, elements[mid]);
       if (c > 0) {
           low = mid + 1;
       } else if (c < 0) {
           high = mid;
       } else if (c === 0) {
           return mid;
       } else {
           throw new Error("Invalid comparator.");
       }
       mid = (low + high) >> 1;
   }
   return (mid * 2) ^ failureXor;
}
