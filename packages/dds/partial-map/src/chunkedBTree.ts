/*!
 * Copyright (c) Microsoft Corporation and contributors. All rights reserved.
 * Licensed under the MIT License.
 */

import { fail } from "assert";
import { assert } from "@fluidframework/common-utils";
import { IChunkedBtree } from "./interfaces";
import { IBtreeLeafNode, ISerializedBtree, IBtreeInteriorNode } from "./persistedTypes";

export class ChunkedBtree<T, THandle> implements IChunkedBtree<T, THandle> {
    private readonly root: IBTreeNode<T, THandle>;

	public constructor(
        public readonly order: number,
        private readonly createHandle: (content: IBtreeLeafNode | IBtreeInteriorNode<THandle>) => Promise<THandle>,
        private readonly resolveHandle: (handle: THandle) => Promise<IBtreeLeafNode | IBtreeInteriorNode<THandle>>,
        root?: IBTreeNode<T, THandle>,
    ) {
        assert(order >= 2, "Order out of bounds");
        this.root = root ?? new LeafyBTreeNode([], [], order, createHandle);
    }

    private cloneWithNewRoot(newRoot: IBTreeNode<T, THandle>): ChunkedBtree<T, THandle> {
        return new ChunkedBtree(this.order, this.createHandle, this.resolveHandle, newRoot);
    }

	public async get(key: string): Promise<T | undefined> {
        return this.root.get(key);
	}

    public async has(key: string): Promise<boolean> {
        return this.root.has(key);
    }

    public async set(key: string, value: T): Promise<ChunkedBtree<T, THandle>> {
        const result = await this.root.set(key, value);
        let newRoot: IBTreeNode<T, THandle>;
        if (Array.isArray(result)) {
            const [nodeA, k, nodeB] = result;
            newRoot = new BTreeNode(
                [k],
                [nodeA, nodeB],
                this.order,
                this.createHandle,
                this.resolveHandle,
            );
        } else {
            newRoot = result;
        }
        return this.cloneWithNewRoot(newRoot);
    }

    public async delete(key: string): Promise<ChunkedBtree<T, THandle>> {
        const newRoot = await this.root.delete(key);
        return this.cloneWithNewRoot(newRoot);
    }

    public flushSync(updates: Iterable<[string, T]>, deletes: Iterable<string>): ISerializedBtree<IBtreeLeafNode> {
        const map = new Map(updates);
        for (const d of deletes) {
            map.delete(d);
        }

        return {
            order: this.order,
            root: {
                keys: [...map.keys()],
                values: [...map.values()],
            },
        };
    }

	public async flush(updates: Iterable<[string, T]>, deletes: Iterable<string>): Promise<ISerializedBtree<THandle>> {
        // eslint-disable-next-line @typescript-eslint/no-this-alias
        let btree: ChunkedBtree<T, THandle> = this;
		for (const [key, value] of updates) {
            btree = await this.set(key, value);
        }

        for (const key of deletes) {
            btree = await this.delete(key);
        }

        return {
            order: btree.order,
            root: await btree.root.upload(),
        };
	}

    public evict(evictionCountHint: number): void {
        this.root.evict({ remaining: evictionCountHint });
    }

    public workingSetSize(): number {
        return this.root.workingSetSize();
    }

    public static async load<T, THandle>(
        { order, root }: ISerializedBtree<THandle | IBtreeLeafNode>,
        createHandle: (content: IBtreeLeafNode | IBtreeInteriorNode<THandle>) => Promise<THandle>,
        resolveHandle: (handle: THandle) => Promise<IBtreeLeafNode | IBtreeInteriorNode<THandle>>,
        isHandle: (handleOrNode: THandle | IBtreeLeafNode) => handleOrNode is THandle,
    ): Promise<ChunkedBtree<T, THandle>> {
        if (isHandle(root)) {
            return new ChunkedBtree(
                order,
                createHandle,
                resolveHandle,
                new LazyBTreeNode(root, order, createHandle, resolveHandle),
            );
        } else {
            const btree = new ChunkedBtree<T, THandle>(order, createHandle, resolveHandle);
            assert(root.keys.length === root.values.length, "Malformed drone; should be same number of keys as values");
            for (const [i, key] of root.keys.entries()) {
                await btree.set(key, root.values[i]);
            }

            return btree;
        }
    }

    public static loadSync<T, THandle>(
        { order, root }: ISerializedBtree<THandle | IBtreeLeafNode>,
        createHandle: (content: IBtreeLeafNode | IBtreeInteriorNode<THandle>) => Promise<THandle>,
        resolveHandle: (handle: THandle) => Promise<IBtreeLeafNode | IBtreeInteriorNode<THandle>>,
        isHandle: (handleOrNode: THandle | IBtreeLeafNode) => handleOrNode is THandle,
    ): ChunkedBtree<T, THandle> {
        if (isHandle(root)) {
            return new ChunkedBtree(
                order,
                createHandle,
                resolveHandle,
                new LazyBTreeNode(root, order, createHandle, resolveHandle),
            );
        } else {
            fail("Cannot synchronously chunk btree.");
        }
    }
}

interface IBTreeNode<T, THandle> {
    has(key: string): Promise<boolean>;
    get(key: string): Promise<T | undefined>;
    set(
        key: string,
        value: T,
    ): Promise<IBTreeNode<T, THandle> | [IBTreeNode<T, THandle>, string, IBTreeNode<T, THandle>]>;
    delete(key: string): Promise<IBTreeNode<T, THandle>>;
    upload(): Promise<THandle>;
    evict(evicted: { remaining: number; }): number;
    workingSetSize(): number;
}

class BTreeNode<T, THandle> {
    public constructor(
        public readonly keys: readonly string[],
        public readonly children: readonly (IBTreeNode<T, THandle>)[],
        public readonly order: number,
        public readonly createHandle: (content: IBtreeInteriorNode<THandle> | IBtreeLeafNode) => Promise<THandle>,
        public readonly resolveHandle: (handle: THandle) => Promise<IBtreeInteriorNode<THandle> | IBtreeLeafNode>,
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
    ): Promise<BTreeNode<T, THandle> | [BTreeNode<T, THandle>, string, BTreeNode<T, THandle>]> {
        for (let i = 0; i < this.children.length; i++) {
            if (i === this.keys.length || key < this.keys[i]) {
                const childResult = await this.children[i].set(key, value);
                if (Array.isArray(childResult)) {
                    // The child split in half
                    const [childA, k, childB] = childResult;
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
                            new BTreeNode(keys, children, this.order, this.createHandle, this.resolveHandle),
                            keys2.splice(0, 1)[0],
                            new BTreeNode(keys2, children2, this.order, this.createHandle, this.resolveHandle),
                        ];
                    }

                    return new BTreeNode(keys, children, this.order, this.createHandle, this.resolveHandle);
                } else {
                    // Replace the child
                    const children = [...this.children];
                    children[i] = childResult;
                    return new BTreeNode(this.keys, children, this.order, this.createHandle, this.resolveHandle);
                }
            }
        }

        throw new Error("Unreachable code");
    }

    public async delete(key: string): Promise<BTreeNode<T, THandle>> {
        for (let i = 0; i < this.children.length; i++) {
            if (i === this.keys.length || key < this.keys[i]) {
                const children = [...this.children];
                children[i] = await this.children[i].delete(key);
                return new BTreeNode(this.keys, children, this.order, this.createHandle, this.resolveHandle);
            }
        }

        throw new Error("Unreachable code");
    }

    public async upload(): Promise<THandle> {
        const worker: IBtreeInteriorNode<THandle> = {
            keys: this.keys,
            children: await Promise.all(this.children.map(async (c) => c.upload())),
        };

        return this.createHandle(worker);
    }

    public evict(evicted: { remaining: number; }): number {
        let unevictedEntriesBelow = 0;
        for (const child of this.children) {
            unevictedEntriesBelow += child.evict(evicted);
            if (evicted.remaining <= 0) {
                break;
            }
        }
        return unevictedEntriesBelow;
    }

    public workingSetSize(): number {
        let entriesBelow = 0;
        for (const child of this.children) {
            entriesBelow += child.workingSetSize();
        }
        return entriesBelow;
    }
}

class LeafyBTreeNode<T, THandle> implements IBTreeNode<T, THandle> {
    public constructor(
        public readonly keys: readonly string[],
        public readonly values: readonly T[],
        public readonly order: number,
        public readonly createHandle: (content: IBtreeLeafNode | IBtreeInteriorNode<THandle>) => Promise<THandle>,
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
    ): Promise<LeafyBTreeNode<T, THandle> | [LeafyBTreeNode<T, THandle>, string, LeafyBTreeNode<T, THandle>]> {
        for (let i = 0; i <= this.keys.length; i++) {
            if (this.keys[i] === key) {
                // Already have a value for this key, so just clone ourselves but replace the value
                const values = [...this.values.slice(0, i), value, ...this.values.slice(i + 1)];
                return new LeafyBTreeNode(this.keys, values, this.order, this.createHandle);
            }
            if (i === this.keys.length || key < this.keys[i]) {
                const keys = insert(this.keys, i, key);
                const values = insert(this.values, i, value);
                if (keys.length >= this.order) {
                    // Split
                    const keys2 = keys.splice(Math.ceil(keys.length / 2), Math.floor(keys.length / 2));
                    const values2 = values.splice(Math.ceil(values.length / 2), Math.floor(values.length / 2));
                    return [
                        new LeafyBTreeNode(keys, values, this.order, this.createHandle),
                        keys2[0],
                        new LeafyBTreeNode(keys2, values2, this.order, this.createHandle),
                    ];
                }
                return new LeafyBTreeNode(keys, values, this.order, this.createHandle);
            }
        }

        throw new Error("Unreachable code");
    }

    public async delete(key: string): Promise<LeafyBTreeNode<T, THandle>> {
        for (let i = 0; i <= this.keys.length; i++) {
            if (this.keys[i] === key) {
                const keys = remove(this.keys, i);
                const values = remove(this.values, i);
                return new LeafyBTreeNode(keys, values, this.order, this.createHandle);
            }
        }

        return this;
    }

    public async upload(): Promise<THandle> {
        const drone: IBtreeLeafNode = {
            keys: this.keys,
            values: this.values,
        };

        return this.createHandle(drone);
    }

    public evict(evicted: { remaining: number; }): number {
        return this.keys.length;
    }

    public workingSetSize(): number {
        return this.keys.length;
    }
}

class LazyBTreeNode<T, THandle> implements IBTreeNode<T, THandle> {
    private node?: IBTreeNode<T, THandle>;

    public constructor(
        private readonly handle: THandle,
        public readonly order: number,
        private readonly createHandle: (content: IBtreeInteriorNode<THandle> | IBtreeLeafNode) => Promise<THandle>,
        private readonly resolveHandle: (handle: THandle) => Promise<IBtreeInteriorNode<THandle> | IBtreeLeafNode>,
    ) {}

    private async load(): Promise<IBTreeNode<T, THandle>> {
        if (this.node === undefined) {
            const loadedNode = await this.resolveHandle(this.handle);
            this.node = this.isLeafNode(loadedNode) ? new LeafyBTreeNode(
                    loadedNode.keys,
                    loadedNode.values,
                    this.order,
                    this.createHandle,
                ) : new BTreeNode(
                    loadedNode.keys,
                    loadedNode.children.map(
                        (handle) => new LazyBTreeNode(handle, this.order, this.createHandle, this.resolveHandle),
                    ),
                    this.order,
                    this.createHandle,
                    this.resolveHandle,
                );
        }

        return this.node;
    }

    public async has(key: string): Promise<boolean> {
        return (await this.load()).has(key);
    }

    public async get(key: string): Promise<T | undefined> {
        return (await this.load()).get(key);
    }

    public async set(
        key: string,
        value: T,
    ): Promise<IBTreeNode<T, THandle> | [IBTreeNode<T, THandle>, string, IBTreeNode<T, THandle>]> {
        return (await this.load()).set(key, value);
    }

    public async delete(key: string): Promise<IBTreeNode<T, THandle>> {
        return (await this.load()).delete(key);
    }

    public async upload(): Promise<THandle> {
        return this.handle;
    }

    private isLeafNode(node: IBtreeInteriorNode<THandle> | IBtreeLeafNode): node is IBtreeLeafNode {
        return (node as IBtreeLeafNode).values !== undefined;
    }

    public evict(evicted: { remaining: number; }): number {
        if (this.node === undefined) {
            return 0;
        }
        const unevictedEntriesBelow = this.node.evict(evicted);
        evicted.remaining -= unevictedEntriesBelow;
        if (evicted.remaining > 0) {
            this.node = undefined;
        }
        return 0;
    }

    public workingSetSize(): number {
        if (this.node === undefined) {
            return 0;
        }
        return this.node.workingSetSize();
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
    return [...array.slice(0, index), ...values, ...array.slice(index)];
}

function remove<T>(array: readonly T[], index: number, count = 1): T[] {
    return [...array.slice(0, index), ...array.slice(index + count)];
}
