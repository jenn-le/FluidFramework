/*!
 * Copyright (c) Microsoft Corporation and contributors. All rights reserved.
 * Licensed under the MIT License.
 */

import { strict as assert } from "assert";
import { ChunkedBTree } from "../chunkedBTree";
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

function mockBTree<T>(order = 3): ChunkedBTree<T, number> {
    const mockHandleMap = new MockHandleMap();
    return new ChunkedBTree<T, number>(
        order,
        async (bee) => mockHandleMap.createHandle(bee),
        async (handle) => mockHandleMap.resolveHandle(handle),
    );
}

describe("BTree", () => {
    it("can set and read a single value", async () => {
        const btree = mockBTree<number>();
        await btree.set("key", 42);
        assert.equal(await btree.get("key"), 42);
    });

    it("can has a single value", async () => {
        const btree = mockBTree<string>();
        assert.equal(await btree.has("key"), false);
        await btree.set("key", "cheezburger");
        assert.equal(await btree.has("key"), true);
    });

    it("can delete a single value", async () => {
        const btree = mockBTree<number>();
        await btree.set("key", 42);
        await btree.delete("key");
        assert.equal(await btree.has("key"), false);
    });

    it("can overwrite a single value", async () => {
        const btree = mockBTree<number>();
        await btree.set("key", 42);
        await btree.set("key", 43);
        assert.equal(await btree.get("key"), 43);
    });

    it("can set many values", async () => {
        const btree = mockBTree<string>();
        for (const key of manyKeys) {
            await btree.set(key, key);
        }

        for (const key of manyKeys) {
            assert.equal(await btree.get(key), key);
        }
    });

    it("can delete many values", async () => {
        const btree = mockBTree<string>();
        for (const key of manyKeys) {
            await btree.set(key, key);
        }

        for (const key of manyKeys) {
            await btree.delete(key);
            assert.equal(await btree.has(key), false);
        }
    });

    it("can load lazily", async () => {
        const mockHandleMap = new MockHandleMap();
        const btree = new ChunkedBTree<string, number>(
            3,
            async (bee) => mockHandleMap.createHandle(bee),
            async (handle) => mockHandleMap.resolveHandle(handle),
        );

        for (const key of manyKeys) {
            await btree.set(key, key);
        }

        const summary = await btree.flush([], []);
        const loadedBTree = await ChunkedBTree.load<unknown, number>(
            summary,
            async (bee) => mockHandleMap.createHandle(bee),
            async (handle) => mockHandleMap.resolveHandle(handle),
            (handle): handle is number => typeof handle === "number",
        );

        for (const key of manyKeys) {
            assert.equal(await loadedBTree.get(key), key);
        }

        for (const key of manyKeys) {
            await btree.delete(key);
            assert.equal(await btree.has(key), false);
        }
    });
});

// eslint-disable-next-line max-len
const manyKeys = "Did you ever hear the tragedy of Darth Plagueis The Wise? I thought not. It’s not a story the Jedi would tell you. It’s a Sith legend. Darth Plagueis was a Dark Lord of the Sith, so powerful and so wise he could use the Force to influence the midichlorians to create life… He had such a knowledge of the dark side that he could even keep the ones he cared about from dying. The dark side of the Force is a pathway to many abilities some consider to be unnatural. He became so powerful… the only thing he was afraid of was losing his power, which eventually, of course, he did. Unfortunately, he taught his apprentice everything he knew, then his apprentice killed him in his sleep. Ironic. He could save others from death, but not himself.".split(" ");
