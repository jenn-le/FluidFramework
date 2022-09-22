/* eslint-disable no-bitwise */
/*!
 * Copyright (c) Microsoft Corporation and contributors. All rights reserved.
 * Licensed under the MIT License.
 */

import { assert } from "@fluidframework/common-utils";
import { IBeeTree, IHandleProvider } from "./interfaces";
import { IDroneBee, IQueenBee, IWorkerBee } from "./persistedTypes";

export class BeeTree<T, THandle> implements IBeeTree<T, THandle>, IHandleProvider {
    private root: IBeeTreeNode<T, THandle>;

	public constructor(
        order: number,
        createHandle: (content: IDroneBee | IWorkerBee<THandle>) => Promise<THandle>,
        resolveHandle: (handle: THandle) => Promise<IDroneBee | IWorkerBee<THandle>>,
    ) {
        assert(order >= 2, "Order out of bounds");
        this.root = new LeafyBeeTreeNode(
            [],
            [],
            order,
            createHandle,
            assertResultAsync(resolveHandle, (bee) => (bee as IDroneBee).values !== undefined),
        );
    }

	public async get(key: string): Promise<T | undefined> {
        return this.root.get(key);
	}

    public async has(key: string): Promise<boolean> {
        return this.root.has(key);
    }

    public async set(key: string, value: T): Promise<void> {
        const result = await this.root.set(key, value);
        if (Array.isArray(result)) {
            const [nodeA, k, nodeB] = result;
            this.root = new BeeTreeNode(
                [k],
                [nodeA, nodeB],
                this.root.order,
                this.root.createHandle,
                assertResultAsync(
                    this.root.resolveHandle,
                    (bee) => (bee as IWorkerBee<THandle>).children !== undefined,
                ),
            );
        } else {
            this.root = result;
        }
    }

    public async delete(key: string): Promise<void> {
        await this.root.delete(key);
    }

	public async summarize(updates: Map<string, T>, deletes: Set<string>): Promise<IQueenBee<THandle>> {
		for (const [key, value] of updates.entries()) {
            await this.set(key, value);
        }

        for (const key of deletes.keys()) {
            await this.delete(key);
        }

        return {
            order: this.root.order,
            root: await this.root.upload(),
        };
	}

    getGcWhitelist(): string[] {
        throw new Error("Method not implemented.");
    }
}

interface IBeeTreeNode<T, THandle> {
    readonly order: number;
    keys: readonly string[];
    has(key: string): Promise<boolean>;
    get(key: string): Promise<T | undefined>;
    set(
        key: string,
        value: T,
    ): Promise<IBeeTreeNode<T, THandle> | [IBeeTreeNode<T, THandle>, string, IBeeTreeNode<T, THandle>]>;
    delete(key: string): Promise<IBeeTreeNode<T, THandle>>; // "delet this"
    upload(): Promise<THandle>;
    createHandle: (content: IDroneBee | IWorkerBee<THandle>) => Promise<THandle>;
    resolveHandle: (handle: THandle) => Promise<IDroneBee | IWorkerBee<THandle>>;
}

class BeeTreeNode<T, THandle> implements IBeeTreeNode<T, THandle> {
    public constructor(
        public readonly keys: readonly string[],
        public readonly children: readonly IBeeTreeNode<T, THandle>[],
        public readonly order: number,
        public readonly createHandle: (content: IDroneBee | IWorkerBee<THandle>) => Promise<THandle>,
        public readonly resolveHandle: (handle: THandle) => Promise<IWorkerBee<THandle>>,
    ) {
        assert(children.length >= 1, "Unexpected empty interior node");
        assert(keys.length === children.length - 1, "Must have exactly one more child than keys");
    }

    public async has(key: string): Promise<boolean> {
        for (let i = 0; i < this.children.length; i++) {
            if (i === this.keys.length || key < this.keys[i]) {
                return this.children[i].has(key);
            }
        }

        throw new Error("Unreachable code");
    }

    public async get(key: string): Promise<T | undefined> {
        for (let i = 0; i < this.children.length; i++) {
            if (i === this.keys.length || key < this.keys[i]) {
                return this.children[i].get(key);
            }
        }

        throw new Error("Unreachable code");
    }

    public async set(
        key: string, value: T,
    ): Promise<BeeTreeNode<T, THandle> | [BeeTreeNode<T, THandle>, string, BeeTreeNode<T, THandle>]> {
        for (let i = 0; i < this.children.length; i++) {
            if (i === this.keys.length || key < this.keys[i]) {
                const childResult = await this.children[i].set(key, value);
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
                            new BeeTreeNode(keys, children, this.order, this.createHandle, this.resolveHandle),
                            keys2.splice(0, 1)[0],
                            new BeeTreeNode(keys2, children2, this.order, this.createHandle, this.resolveHandle),
                        ];
                    }
                } else {
                    // Replace the child
                    const children = [...this.children];
                    children[i] = childResult;
                    return new BeeTreeNode(this.keys, children, this.order, this.createHandle, this.resolveHandle);
                }
            }
        }

        throw new Error("Unreachable code");
    }

    public async delete(key: string): Promise<BeeTreeNode<T, THandle>> {
        for (let i = 0; i < this.children.length; i++) {
            if (i === this.keys.length || key < this.keys[i]) {
                const children = [...this.children];
                children[i] = await this.children[i].delete(key);
                return new BeeTreeNode(this.keys, children, this.order, this.createHandle, this.resolveHandle);
            }
        }

        throw new Error("Unreachable code");
    }

    public async upload(): Promise<THandle> {
        const worker: IWorkerBee<THandle> = {
            keys: this.keys,
            children: await Promise.all(this.children.map(async (c) => c.upload())),
        };

        return this.createHandle(worker);
    }
}

class LeafyBeeTreeNode<T, THandle> implements IBeeTreeNode<T, THandle> {
    public constructor(
        public readonly keys: readonly string[],
        public readonly values: readonly T[],
        public readonly order: number,
        public readonly createHandle: (content: IDroneBee | IWorkerBee<THandle>) => Promise<THandle>,
        public readonly resolveHandle: (handle: THandle) => Promise<IDroneBee>,
    ) {
        assert(keys.length === values.length, "Invalid keys or values");
    }

    public async has(key: string): Promise<boolean> {
        for (const k of this.keys) {
            if (k === key) {
                return true;
            }
        }

        return false;
    }

    public async get(key: string): Promise<T | undefined> {
        for (let i = 0; i < this.keys.length; i++) {
            if (this.keys[i] === key) {
                return this.values[i];
            }
        }

        return undefined;
    }

    public async set(
        key: string,
        value: T,
    ): Promise<LeafyBeeTreeNode<T, THandle> | [LeafyBeeTreeNode<T, THandle>, string, LeafyBeeTreeNode<T, THandle>]> {
        for (let i = 0; i <= this.keys.length; i++) {
            if (this.keys[i] === key[i]) {
                // Already have a value for this key, so just clone ourselves but replace the value
                const values = [...this.values.slice(0, i), value, ...this.values.slice(i + 1)];
                return new LeafyBeeTreeNode(this.keys, values, this.order, this.createHandle, this.resolveHandle);
            }
            if (i === this.keys.length || key < this.keys[i]) {
                const keys = insert(this.keys, i, key);
                const values = insert(this.values, i, value);
                if (keys.length >= this.order) {
                    // Split
                    const keys2 = keys.splice(Math.floor(keys.length / 2), Math.ceil(keys.length / 2));
                    const values2 = values.splice(Math.ceil(values.length / 2), Math.floor(values.length / 2));
                    return [
                        new LeafyBeeTreeNode(keys, values, this.order, this.createHandle, this.resolveHandle),
                        keys2.splice(0, 1)[0],
                        new LeafyBeeTreeNode(keys2, values2, this.order, this.createHandle, this.resolveHandle),
                    ];
                }
                return new LeafyBeeTreeNode(keys, values, this.order, this.createHandle, this.resolveHandle);
            }
        }

        throw new Error("Unreachable code");
    }

    public async delete(key: string): Promise<LeafyBeeTreeNode<T, THandle>> {
        for (let i = 0; i <= this.keys.length; i++) {
            if (this.keys[i] === key) {
                const keys = remove(this.keys, i);
                const values = remove(this.values, i);
                return new LeafyBeeTreeNode(keys, values, this.order, this.createHandle, this.resolveHandle);
            }
        }

        return this;
    }

    public async upload(): Promise<THandle> {
        const drone: IDroneBee = {
            keys: this.keys,
            values: this.values,
        };

        return this.createHandle(drone);
    }
}

// /**
// * The value xor'd with the result index when a search fails.
// */
// const failureXor = -1;

// /**
// * Performs a binary search on the sorted array.
// * @returns the index of the key for `search`, or (if not present) the index it would have been inserted into xor'd
// * with `failureXor`. Note that negating is not an adequate solution as that could result in -0.
// */
// function search<T>(
//    elements: readonly T[],
//    target: T,
//    comparator: (a: T, b: T) => number,
// ): number | undefined {
//    let low = 0;
//    let high = elements.length - 1;
//    let mid = high >> 1;
//    while (low < high) {
//        const c = comparator(target, elements[mid]);
//        if (c > 0) {
//            low = mid + 1;
//        } else if (c < 0) {
//            high = mid;
//        } else if (c === 0) {
//            return mid;
//        } else {
//            throw new Error("Invalid comparator.");
//        }
//        mid = (low + high) >> 1;
//    }
//    return (mid * 2) ^ failureXor;
// }

function insert<T>(array: readonly T[], index: number, ...values: T[]): T[] {
    return [...array.slice(0, index), ...values, ...array.slice(index + 1)];
}

function remove<T>(array: readonly T[], index: number, count = 1): T[] {
    return [...array.slice(0, index), ...array.slice(index + count)];
}

/**
 * Convert an async function to one with a narrower return type
 */
 function assertResultAsync<TArgs extends unknown[], TResult, TAssertResult extends TResult>(
    fn: (...args: TArgs) => Promise<TResult>,
    ass: (ret: TResult) => asserts ret is TAssertResult,
): (...args: TArgs) => Promise<TAssertResult> {
    return async (...args: TArgs) => {
        const ret = await fn(...args);
        ass(ret);
        return ret;
    };
}
