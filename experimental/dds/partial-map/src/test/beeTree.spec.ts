/*!
 * Copyright (c) Microsoft Corporation and contributors. All rights reserved.
 * Licensed under the MIT License.
 */

import { strict as assert } from "assert";
import { ChunkedBtree } from "../chunkedBTree";
import { IBtreeLeafNode, IBtreeInteriorNode } from "../persistedTypes";

class MockHandleMap {
    private readonly map = new Map<number, IBtreeInteriorNode<number> | IBtreeLeafNode>();
    private count = 0;

    public createHandle(t: IBtreeInteriorNode<number> | IBtreeLeafNode): number {
        this.map.set(this.count, t);
        return this.count++;
    }

    public resolveHandle(handle: number): IBtreeInteriorNode<number> | IBtreeLeafNode {
        assert(this.map.has(handle), "Invalid mock handle");
        const bee = this.map.get(handle);
        assert(bee !== undefined, "Map contains undefined as a value");
        return bee;
    }
}

class MockValueHandle {
    public constructor(public readonly id: number) {}

    public valueOf(): number {
        return this.id;
    }
}

function mockBTree<T extends number | string | boolean | MockValueHandle>(
    order = 3,
): ChunkedBtree<T, number, MockValueHandle> {
    const mockHandleMap = new MockHandleMap();
    return ChunkedBtree.create<T, number, MockValueHandle>(
        order,
        {
            createHandle: async (bee) => mockHandleMap.createHandle(bee),
            resolveHandle: async (handle) => mockHandleMap.resolveHandle(handle),
            compareHandles: (a, b) => a.valueOf() - b.valueOf(),
            discoverHandles: (value) => {
                if (typeof value === "object" && value instanceof MockValueHandle) {
                    return [value];
                }
                return [];
            },
        },
    );
}

describe("BTree", () => {
    it("can set and read a single value", async () => {
        let btree = mockBTree<number>();
        btree = await btree.set("key", 42, [], []);
        assert.equal(await btree.get("key"), 42);
    });

    it("can has a single value", async () => {
        let btree = mockBTree<string>();
        assert.equal(await btree.has("key"), false);
        btree = await btree.set("key", "cheezburger", [], []);
        assert.equal(await btree.has("key"), true);
    });

    it("can delete a single value", async () => {
        let btree = mockBTree<number>();
        btree = await btree.set("key", 42, [], []);
        btree = await btree.delete("key", []);
        assert.equal(await btree.has("key"), false);
    });

    it("can overwrite a single value", async () => {
        let btree = mockBTree<number>();
        btree = await btree.set("key", 42, [], []);
        btree = await btree.set("key", 43, [], []);
        assert.equal(await btree.get("key"), 43);
    });

    it("can set many values", async () => {
        let btree = mockBTree<string>();
        for (const key of manyKeys) {
            btree = await btree.set(key, key, [], []);
        }

        for (const key of manyKeys) {
            assert.equal(await btree.get(key), key);
        }
    });

    it("can delete many values", async () => {
        let btree = mockBTree<string>();
        for (const key of manyKeys) {
            btree = await btree.set(key, key, [], []);
        }

        for (const key of manyKeys) {
            btree = await btree.delete(key, []);
            assert.equal(await btree.has(key), false);
        }
    });

    async function flushAndLoad<T>(
        btree: ChunkedBtree<T, number, MockValueHandle>): Promise<ChunkedBtree<T, number, MockValueHandle>> {
        const update = await btree.flush([], []);
        return btree.update(update);
    }

    it("can load lazily", async () => {
        let btree = mockBTree<string>();

        for (const key of manyKeys) {
            btree = await btree.set(key, key, [], []);
        }

        let loadedBTree = await flushAndLoad(btree);
        for (const key of manyKeys) {
            assert.equal(await loadedBTree.get(key), key);
        }

        for (const key of manyKeys) {
            loadedBTree = await loadedBTree.delete(key, []);
            assert.equal(await loadedBTree.has(key), false);
        }
    });

    function assertIsWithin(a: number, b: number, within: number): void {
        assert(Math.abs(a - b) <= within);
    }

    async function readAllKeys(
        btree: ChunkedBtree<string, number, MockValueHandle>,
        keys: string[],
    ): Promise<string[]> {
        const values: string[] = [];
        for (const key of keys) {
            assert.equal(await btree.get(key), key);
            values.push(key);
        }
        return values;
    }

    async function insert(
        btree: ChunkedBtree<string, number, MockValueHandle>,
        count: number,
    ): Promise<[ChunkedBtree<string, number, MockValueHandle>, string[]]> {
        let out = btree;
        const keys: string[] = [];
        for (let i = 0; i < count; i++) {
            const key = i.toString();
            keys.push(key);
            out = await out.set(key, key, [], []);
        }
        return [out, keys];
    }

    it("can evict loaded nodes", async () => {
        // eslint-disable-next-line prefer-const
        let [btree, keys] = await insert(mockBTree<string>(), 1000);
        assert.equal(btree.workingSetSize(), keys.length);

        btree.evict(keys.length);
        assert.equal(btree.workingSetSize(), keys.length);

        btree = await flushAndLoad(btree);
        assert.equal(btree.workingSetSize(), 0);
        await readAllKeys(btree, keys);
        assert.equal(btree.workingSetSize(), keys.length);

        btree.evict(keys.length);
        assertIsWithin(btree.workingSetSize(), 0, btree.order);

        await readAllKeys(btree, keys);
        assert.equal(btree.workingSetSize(), keys.length);

        const toEvict = Math.round(keys.length / 2);
        btree.evict(toEvict);
        assertIsWithin(btree.workingSetSize(), keys.length - toEvict, btree.order);
    });

    async function expectHandles(
        btree: ChunkedBtree<string, number, MockValueHandle>,
        modifies: number[],
        deletes: number[],
        added: number,
        deleted: number,
    ): Promise<ChunkedBtree<string, number, MockValueHandle>> {
        const update = await btree.flush(
            modifies.map((num) => [num.toString(), num.toString()]), deletes.map((num) => num.toString()));
        assert.equal(update.newHandles.length, added);
        assert.equal(update.deletedHandles.length, deleted);
        return btree.update(update);
    }

    it("can track handles for btree nodes", async () => {
        let btree = mockBTree<string>();
        btree = await expectHandles(btree, [1, 2, 3, 4, 5, 6, 7, 8], [], 7, 0);
        btree = await expectHandles(btree, [1], [], 3, 3);
        btree = await expectHandles(btree, [], [7], 3, 3);
        btree = await expectHandles(btree, [9], [], 3, 3);
        btree = await expectHandles(btree, [10], [], 4, 3);
        const allKeys = [1, 2, 3, 4, 5, 6, 8, 9, 10].map((num) => num.toString());
        const values = await readAllKeys(btree, allKeys);
        assert.deepEqual(values, allKeys);
    });

    it("can track handles for values", async () => {
        let btree = mockBTree<string | MockValueHandle>();
        btree = await flushAndLoad(btree);
        const addedHandles = [];
        const deletedHandles = [];
        btree = await btree.set("test", new MockValueHandle(42), addedHandles, deletedHandles);
        assert.deepEqual(addedHandles.splice(0)[0], new MockValueHandle(42));
        assert.equal(deletedHandles.splice(0).length, 1);
        btree = await btree.delete("test", deletedHandles);
        assert.deepEqual(deletedHandles[0], new MockValueHandle(42));
    });
});

// eslint-disable-next-line max-len
const manyKeys = "Did you ever hear the tragedy of Darth Plagueis The Wise? I thought not. It’s not a story the Jedi would tell you. It’s a Sith legend. Darth Plagueis was a Dark Lord of the Sith, so powerful and so wise he could use the Force to influence the midichlorians to create life… He had such a knowledge of the dark side that he could even keep the ones he cared about from dying. The dark side of the Force is a pathway to many abilities some consider to be unnatural. He became so powerful… the only thing he was afraid of was losing his power, which eventually, of course, he did. Unfortunately, he taught his apprentice everything he knew, then his apprentice killed him in his sleep. Ironic. He could save others from death, but not himself.".split(" ");
