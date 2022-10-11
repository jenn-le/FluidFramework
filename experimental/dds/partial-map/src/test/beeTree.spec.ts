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

    it("can evict loaded nodes", async () => {
        let btree = mockBTree<string>();
        assert.equal(btree.workingSetSize(), 0);

        const keys: string[] = [];
        for (let i = 0; i < 1000; i++) {
            const key = i.toString();
            keys.push(key);
            btree = await btree.set(key, key, [], []);
        }

        const readAllKeys = async () => {
            for (const key of keys) {
                assert.equal(await btree.get(key), key);
            }
        };

        assert.equal(btree.workingSetSize(), keys.length);

        btree.evict(keys.length);
        assert.equal(btree.workingSetSize(), keys.length);

        btree = await flushAndLoad(btree);
        assert.equal(btree.workingSetSize(), 0);
        await readAllKeys();
        assert.equal(btree.workingSetSize(), keys.length);

        btree.evict(keys.length);
        assertIsWithin(btree.workingSetSize(), 0, btree.order);

        await readAllKeys();
        assert.equal(btree.workingSetSize(), keys.length);

        const toEvict = Math.round(keys.length / 2);
        btree.evict(toEvict);
        assertIsWithin(btree.workingSetSize(), keys.length - toEvict, btree.order);
    });
});

// eslint-disable-next-line max-len
const manyKeys = "Did you ever hear the tragedy of Darth Plagueis The Wise? I thought not. It’s not a story the Jedi would tell you. It’s a Sith legend. Darth Plagueis was a Dark Lord of the Sith, so powerful and so wise he could use the Force to influence the midichlorians to create life… He had such a knowledge of the dark side that he could even keep the ones he cared about from dying. The dark side of the Force is a pathway to many abilities some consider to be unnatural. He became so powerful… the only thing he was afraid of was losing his power, which eventually, of course, he did. Unfortunately, he taught his apprentice everything he knew, then his apprentice killed him in his sleep. Ironic. He could save others from death, but not himself.".split(" ");
