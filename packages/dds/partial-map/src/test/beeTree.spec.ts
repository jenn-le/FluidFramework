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
        const beeTree = mockBeeTree();
        await beeTree.set("key", 42);
        assert.equal(await beeTree.get("key"), 42);
    });

    it("can has a single value", async () => {
        const beeTree = mockBeeTree();
        assert.equal(await beeTree.has("key"), false);
        await beeTree.set("key", "cheezburger");
        assert.equal(await beeTree.has("key"), true);
    });

    it("can delete a single value", async () => {
        const beeTree = mockBeeTree();
        await beeTree.set("key", 42);
        await beeTree.delete("key");
        assert.equal(await beeTree.has("key"), false);
    });

    it("can overwrite a single value", async () => {
        const beeTree = mockBeeTree();
        await beeTree.set("key", 42);
        await beeTree.set("key", 43);
        assert.equal(await beeTree.get("key"), 43);
    });

    it("can set many values", async () => {
        const beeTree = mockBeeTree();
        const values: number[] = [];
        for (let i = 0; i < 100; i++) {
            values.push(i);
        }
        await beeTree.set("key", 42);
        assert.equal(await beeTree.get("key"), 42);
    });
});
