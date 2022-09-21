/* eslint-disable no-bitwise */
/*!
 * Copyright (c) Microsoft Corporation and contributors. All rights reserved.
 * Licensed under the MIT License.
 */

import { assert, TypedEventEmitter } from "@fluidframework/common-utils";
import { IBeeTree, IBeeTreeEvents, IHandleProvider } from "./interfaces";
import { IQueenBee } from "./persistedTypes";

export class BeeTree<T> extends TypedEventEmitter<IBeeTreeEvents>, implements IBeeTree<T>, IHandleProvider {
    private readonly map = new Map<string, T>();

	constructor(node: IQueenBee) {
        super();
    }

	async get(key: string): Promise<T | undefined> {
        return this.map.get(key);
	}

    async has(key: string): Promise<boolean> {
        throw new Error("Method not implemented.");
    }

    public clear(): void {
        throw new Error("Method not implemented");
    }

	async summarize(updates: Map<string, T>, deletes: Set<string>): Promise<IQueenBee> {
        const queen: IQueenBee = {
            keys: [],
            children: [],
        };

		for (const [key, value] of updates.entries()) {
            if (!this.map.set(key, value)) {
                throw new Error("Set failed");
            }
        }

        for (const key of deletes.keys()) {
            if (!this.map.delete(key)) {
                throw new Error("Delete failed");
            }
        }

        return queen;
	}

    getGcWhitelist(): string[] {
        throw new Error("Method not implemented.");
    }
}

interface IBeeTreeNode<T> {
    keys: readonly string[];
    get(key: string): T | undefined;
    set(key: string, value: T): IBeeTreeNode<T> | [IBeeTreeNode<T>, IBeeTreeNode<T>];
}

class BeeTreeNode<T> implements IBeeTreeNode<T> {
    public constructor(
        public readonly keys: readonly string[],
        // A node with no children is a "leafy node" (its would-be children are leaves)
        public readonly children: readonly IBeeTreeNode<T>[],
    ) {}

    public get(key: string): T | undefined {
        for (let i = 0; i < this.keys.length; i++) {
            if (key < this.keys[i]) {
                return this.children[i].get(key);
            }
        }

        return this.children?.[this.children.length - 1].get(key);
    }

    public set(key: string, value: T): BeeTreeNode<T> | [BeeTreeNode<T>, BeeTreeNode<T>] {
        for (let i = 0; i <= this.keys.length; i++) {
            if (i === this.keys.length || key < this.keys[i]) {
                const childResult = this.children[i].set(key, value);
                if (Array.isArray(childResult)) {
                    // The child split in half
                    const [childA, childB] = childResult;
                    const keys = insert(this.keys, i, childB.keys[0]);
                    const children = insert(this.children, i, childA, childB);
                    if (keys.length >= 32) {
                        // Split
                        const keys2 = keys.splice(Math.ceil(keys.length / 2), Math.floor(keys.length / 2));
                        const children2 = children.splice(
                            Math.ceil(children.length / 2),
                            Math.floor(children.length / 2),
                        );

                        return [
                            new BeeTreeNode(keys, children),
                            new BeeTreeNode(keys2, children2),
                        ];
                    }
                } else {
                    // Replace the child
                    const children = [...this.children];
                    children[i] = childResult;
                    return new BeeTreeNode(this.keys, children);
                }
            }
        }

        throw new Error("Unreachable code");
    }
}

class LeafyBTreeNode<T> implements IBeeTreeNode<T> {
    public constructor(
        public readonly keys: readonly string[],
        public readonly values: readonly T[],
    ) {
        assert(keys.length > 0, "Must have at least one key");
        assert(keys.length === values.length, "Invalid keys or values");
    }

    get(key: string): T | undefined {
        for (let i = 0; i < this.keys.length; i++) {
            if (key === this.keys[i]) {
                return this.values[i];
            }
        }

        return undefined;
    }

    set(key: string, value: T): LeafyBTreeNode<T> | [LeafyBTreeNode<T>, LeafyBTreeNode<T>] {
        for (let i = 0; i <= this.keys.length; i++) {
            if (key === this.keys[i]) {
                // Already have a value for this key, so just clone ourselves but replace the value
                const values = [...this.values.slice(0, i), value, ...this.values.slice(i + 1)];
                return new LeafyBTreeNode(this.keys, values);
            }
            if (i === this.keys.length || key < this.keys[i]) {
                const keys = insert(this.keys, i, key);
                const values = insert(this.values, i, value);
                if (keys.length >= 32) {
                    // Split
                    const keys2 = keys.splice(Math.ceil(keys.length / 2), Math.floor(keys.length / 2));
                    const values2 = values.splice(Math.ceil(values.length / 2), Math.floor(values.length / 2));
                    return [
                        new LeafyBTreeNode(keys, values),
                        new LeafyBTreeNode(keys2, values2),
                    ];
                }
                return new LeafyBTreeNode(keys, values);
            }
        }

        throw new Error("Unreachable code");
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
