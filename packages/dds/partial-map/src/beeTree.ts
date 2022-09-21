/* eslint-disable no-bitwise */
/*!
 * Copyright (c) Microsoft Corporation and contributors. All rights reserved.
 * Licensed under the MIT License.
 */

import { IEvent, IEventProvider } from "@fluidframework/common-definitions";
import { assert, TypedEventEmitter } from "@fluidframework/common-utils";
import { IFluidHandle } from "@fluidframework/core-interfaces";
import { IBeeTree, IHandleProvider } from "./interfaces";
import { IQueenBee } from "./persistedTypes";

interface BeeTreeEvents extends IEvent {
    (event: "handleAdded", listener: (handle: IFluidHandle) => void): void;
    (event: "handleRemoved", listener: (handle: IFluidHandle) => void): void;
}

export class BeeTree<T> extends TypedEventEmitter<BeeTreeEvents>, implements IBeeTree<T>, IHandleProvider {
    private readonly map = new Map<string, T>();
    private readonly gcWhitelist = new Set<string>();

	constructor(node: IQueenBee) {
        super();
    }

	async get(key: string): Promise<T | undefined> {
        return this.map.get(key);
	}

    async has(key: string): Promise<boolean> {
        throw new Error("Method not implemented.");
    }

	async summarize(updates: Map<string, T>, deletes: Set<string>): Promise<[IQueenBee, string[]]> {
        // This is kept in anticipation of a GC blacklist API
        // TODO: this should be moved to wherever we do the actual reuploads
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

    getGcWhitelist(): string[] {
        return Array.from(this.gcWhitelist.values());
    }
}

class BTreeNode<T> {
    public constructor(
        public readonly keys: readonly string[],
        public readonly values: readonly T[],
        // A node with no children is a "leafy node" (its would-be children are leaves)
        public readonly children?: readonly BTreeNode<T>[],
    ) {
        assert(values.length === keys.length, "Invalid keys or values");
        if (children !== undefined) {
            assert(keys.length === children.length - 1, "Invalid keys or children");
        }
    }

    public get(key: string): T | undefined {
        for (let i = 0; i < this.keys.length; i++) {
            if (key === this.keys[i]) {
                return this.values[i];
            }
            if (key < this.keys[i]) {
                return this.children?.[i].get(key);
            }
        }

        return this.children?.[this.children.length - 1].get(key);
    }

    public set(key: string, value: T): BTreeNode<T> | [BTreeNode<T>, BTreeNode<T>] {
        for (let i = 0; i < this.keys.length; i++) {
            if (key === this.keys[i]) {
                // Already have a value for this key, so just clone ourselves but replace the value
                const values = [...this.values.slice(0, i), value, ...this.values.slice(i + 1)];
                return new BTreeNode(this.keys, values, this.children);
            }
            if (key < this.keys[i]) {
                if (this.children === undefined) {
                    // We're a leafy node, so we just want to add the key value pair
                    const keys = insert(this.keys, i, key);
                    const values = insert(this.values, i, value);
                    if (keys.length >= 32) {
                        // Split
                        const keys2 = keys.splice(Math.ceil(keys.length / 2), Math.floor(keys.length / 2));
                        const values2 = values.splice(Math.ceil(values.length / 2), Math.floor(values.length / 2));
                        return [
                            new BTreeNode(keys, values),
                            new BTreeNode(keys2, values2),
                        ];
                    }
                    return new BTreeNode(keys, values);
                }
                // We're a (non-leafy) interior node, so delegate the operation to a child
                const childResult = this.children[i].set(key, value);
                if (Array.isArray(childResult)) {
                    // Child split
                    const [childA, childB] = childResult;
                    const keys = insert(this.keys, i,
                    const children = insert(this.children, i, ...childResult);
                } else {
                    const children = [...this.children];
                    children[i] = childResult;
                    return new BTreeNode(this.keys, this.values, children);
                }
            }
        }

        return this.children[this.children.length - 1].set(key, value);
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

function insert<T>(array: readonly T[], index: number, ...values: T[]): T[] {
    return [...array.slice(0, index), ...values, ...array.slice(index + 1)];
}
