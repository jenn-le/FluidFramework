/* eslint-disable no-bitwise */
/*!
 * Copyright (c) Microsoft Corporation and contributors. All rights reserved.
 * Licensed under the MIT License.
 */

import { assert, TypedEventEmitter } from "@fluidframework/common-utils";
import { ISerializedHandle } from "@fluidframework/shared-object-base";
import { IBeeTree, IBeeTreeEvents, IHandleProvider } from "./interfaces";
import { IDroneBee, IQueenBee, IWorkerBee } from "./persistedTypes";

export class BeeTree<T> extends TypedEventEmitter<IBeeTreeEvents> implements IBeeTree<T>, IHandleProvider {
    private root: IBeeTreeNode<T>;
    private readonly order: number;

	public constructor(order: number) {
        super();
        this.order = order;
        this.root = new LeafyBeeTreeNode([], [], this.order);
    }

	public async get(key: string): Promise<T | undefined> {
        return this.root.get(key);
	}

    public async has(key: string): Promise<boolean> {
        return this.root.has(key);
    }

    public clear(): void {
        this.root = new LeafyBeeTreeNode([], [], this.order);
    }

    private set(key: string, value: T): void {
        const result = this.root.set(key, value);
        if (Array.isArray(result)) {
            const [nodeA, k, nodeB] = result;
            this.root = new BeeTreeNode([k], [nodeA, nodeB], this.order);
        } else {
            this.root = result;
        }
    }

	public async summarize(updates: Map<string, T>, deletes: Set<string>): Promise<IQueenBee> {
        const queen: IQueenBee = {
            keys: [],
            children: [],
        };

		for (const [key, value] of updates.entries()) {
            if (!this.set(key, value)) {
                throw new Error("Set failed");
            }
        }

        for (const key of deletes.keys()) {
            if (!this.delete(key)) {
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
    readonly order: number;
    keys: readonly string[];
    has(key: string): boolean;
    get(key: string): T | undefined;
    set(key: string, value: T): IBeeTreeNode<T> | [IBeeTreeNode<T>, string, IBeeTreeNode<T>];
    delete(key: string): IBeeTreeNode<T>; // "delet this"
    upload(): Promise<ISerializedHandle>;
}

class BeeTreeNode<T> implements IBeeTreeNode<T> {
    public constructor(
        public readonly keys: readonly string[],
        public readonly children: readonly IBeeTreeNode<T>[],
        public readonly order: number,
        private readonly getHandle: (content: unknown) => Promise<ISerializedHandle>,
    ) {
        assert(children.length >= 1, "Unexpected empty interior node");
        assert(keys.length === children.length - 1, "Must have exactly one more child than keys");
    }

    public has(key: string): boolean {
        for (let i = 0; i < this.children.length; i++) {
            if (i === this.keys.length || key < this.keys[i]) {
                return this.children[i].has(key);
            }
        }

        throw new Error("Unreachable code");
    }

    public get(key: string): T | undefined {
        for (let i = 0; i < this.children.length; i++) {
            if (i === this.keys.length || key < this.keys[i]) {
                return this.children[i].get(key);
            }
        }

        throw new Error("Unreachable code");
    }

    public set(key: string, value: T): BeeTreeNode<T> | [BeeTreeNode<T>, string, BeeTreeNode<T>] {
        for (let i = 0; i < this.children.length; i++) {
            if (i === this.keys.length || key < this.keys[i]) {
                const childResult = this.children[i].set(key, value);
                if (Array.isArray(childResult)) {
                    // The child split in half
                    const [childA, k, childB] = childResult; // TODO
                    const keys = insert(this.keys, i, k);
                    const children = insert(remove(this.children, i), i, childA, childB);
                    if (keys.length >= this.order) {
                        // Split
                        const keys2 = keys.splice(Math.floor(keys.length / 2), Math.ceil(keys.length / 2));
                        const children2 = children.splice(
                            Math.ceil(children.length / 2),
                            Math.floor(children.length / 2),
                        );

                        return [
                            new BeeTreeNode(keys, children, this.order, this.getHandle),
                            keys2.splice(0, 1)[0],
                            new BeeTreeNode(keys2, children2, this.order, this.getHandle),
                        ];
                    }
                } else {
                    // Replace the child
                    const children = [...this.children];
                    children[i] = childResult;
                    return new BeeTreeNode(this.keys, children, this.order, this.getHandle);
                }
            }
        }

        throw new Error("Unreachable code");
    }

    public delete(key: string): BeeTreeNode<T> {
        for (let i = 0; i < this.children.length; i++) {
            if (i === this.keys.length || key < this.keys[i]) {
                const children = [...this.children];
                children[i] = this.children[i].delete(key);
                return new BeeTreeNode(this.keys, children, this.order, this.getHandle);
            }
        }

        throw new Error("Unreachable code");
    }

    public async upload(): Promise<ISerializedHandle> {
        const worker: IWorkerBee = {
            keys: this.keys,
            children: await Promise.all(this.children.map(async (c) => c.upload())),
        };

        return this.getHandle(worker);
    }
}

class LeafyBeeTreeNode<T> implements IBeeTreeNode<T> {
    public constructor(
        public readonly keys: readonly string[],
        public readonly values: readonly T[],
        public readonly order: number,
        private readonly getHandle: (content: unknown) => Promise<ISerializedHandle>,
    ) {
        assert(keys.length === values.length, "Invalid keys or values");
    }

    public has(key: string): boolean {
        for (const k of this.keys) {
            if (k === key) {
                return true;
            }
        }

        return false;
    }

    public get(key: string): T | undefined {
        for (let i = 0; i < this.keys.length; i++) {
            if (this.keys[i] === key) {
                return this.values[i];
            }
        }

        return undefined;
    }

    public set(key: string, value: T): LeafyBeeTreeNode<T> | [LeafyBeeTreeNode<T>, string, LeafyBeeTreeNode<T>] {
        for (let i = 0; i <= this.keys.length; i++) {
            if (this.keys[i] === key[i]) {
                // Already have a value for this key, so just clone ourselves but replace the value
                const values = [...this.values.slice(0, i), value, ...this.values.slice(i + 1)];
                return new LeafyBeeTreeNode(this.keys, values, this.order, this.getHandle);
            }
            if (i === this.keys.length || key < this.keys[i]) {
                const keys = insert(this.keys, i, key);
                const values = insert(this.values, i, value);
                if (keys.length >= this.order) {
                    // Split
                    const keys2 = keys.splice(Math.floor(keys.length / 2), Math.ceil(keys.length / 2));
                    const values2 = values.splice(Math.ceil(values.length / 2), Math.floor(values.length / 2));
                    return [
                        new LeafyBeeTreeNode(keys, values, this.order, this.getHandle),
                        keys2.splice(0, 1)[0],
                        new LeafyBeeTreeNode(keys2, values2, this.order, this.getHandle),
                    ];
                }
                return new LeafyBeeTreeNode(keys, values, this.order, this.getHandle);
            }
        }

        throw new Error("Unreachable code");
    }

    public delete(key: string): LeafyBeeTreeNode<T> {
        for (let i = 0; i <= this.keys.length; i++) {
            if (this.keys[i] === key) {
                const keys = remove(this.keys, i);
                const values = remove(this.values, i);
                return new LeafyBeeTreeNode(keys, values, this.order, this.getHandle);
            }
        }

        return this;
    }

    public async upload(): Promise<ISerializedHandle> {
        const drone: IDroneBee = {
            keys: this.keys,
            values: this.values,
        };

        return this.getHandle(drone);
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

function remove<T>(array: readonly T[], index: number, count = 1): T[] {
    return [...array.slice(0, index), ...array.slice(index + count)];
}
