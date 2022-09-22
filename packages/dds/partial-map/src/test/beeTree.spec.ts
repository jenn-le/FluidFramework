/*!
 * Copyright (c) Microsoft Corporation and contributors. All rights reserved.
 * Licensed under the MIT License.
 */

import { strict as assert } from "assert";
import { BeeTree } from "../beeTree";
import { IDroneBee, IWorkerBee } from "../persistedTypes";

class MockHandleMap {
    private readonly map = new Map<number, IWorkerBee<number> | IDroneBee>();
    private count = 0;

    public createHandle(t: IWorkerBee<number> | IDroneBee): number {
        this.map.set(this.count, t);
        return this.count++;
    }

    public resolveHandle(handle: number): IWorkerBee<number> | IDroneBee {
        assert(this.map.has(handle), "Invalid mock handle");
        const bee = this.map.get(handle);
        assert(bee !== undefined, "Map contains undefined as a value");
        return bee;
    }
}

function mockBeeTree<T>(order = 3): BeeTree<T, number> {
    const mockHandleMap = new MockHandleMap();
    // eslint-disable-next-line @typescript-eslint/no-unsafe-return
    return new BeeTree<T, number>(
        order,
        async (bee) => mockHandleMap.createHandle(bee),
        // eslint-disable-next-line @typescript-eslint/no-unsafe-return
        async (handle) => mockHandleMap.resolveHandle(handle),
    );
}

describe("BeeTree", () => {
    it("can set and read a single value", async () => {
        const beeTree = mockBeeTree<number>();
        await beeTree.set("key", 42);
        assert.equal(await beeTree.get("key"), 42);
    });

    it("can has a single value", async () => {
        const beeTree = mockBeeTree<string>();
        assert.equal(await beeTree.has("key"), false);
        await beeTree.set("key", "cheezburger");
        assert.equal(await beeTree.has("key"), true);
    });

    it("can delete a single value", async () => {
        const beeTree = mockBeeTree<number>();
        await beeTree.set("key", 42);
        await beeTree.delete("key");
        assert.equal(await beeTree.has("key"), false);
    });

    it("can overwrite a single value", async () => {
        const beeTree = mockBeeTree<number>();
        await beeTree.set("key", 42);
        await beeTree.set("key", 43);
        assert.equal(await beeTree.get("key"), 43);
    });

    it("can set many values", async () => {
        const beeTree = mockBeeTree<string>();
        for (const key of manyKeys) {
            await beeTree.set(key, key);
        }

        for (const key of manyKeys) {
            assert.equal(await beeTree.get(key), key);
        }
    });

    it("can delete many values", async () => {
        const beeTree = mockBeeTree<string>();
        for (const key of manyKeys) {
            await beeTree.set(key, key);
        }

        for (const key of manyKeys) {
            await beeTree.delete(key);
            assert.equal(await beeTree.has(key), false);
        }
    });
});

// eslint-disable-next-line max-len
const manyKeys = "Did you ever hear the tragedy of Darth Plagueis The Wise? I thought not. It’s not a story the Jedi would tell you. It’s a Sith legend. Darth Plagueis was a Dark Lord of the Sith, so powerful and so wise he could use the Force to influence the midichlorians to create life… He had such a knowledge of the dark side that he could even keep the ones he cared about from dying. The dark side of the Force is a pathway to many abilities some consider to be unnatural. He became so powerful… the only thing he was afraid of was losing his power, which eventually, of course, he did. Unfortunately, he taught his apprentice everything he knew, then his apprentice killed him in his sleep. Ironic. He could save others from death, but not himself.".split(" ");
