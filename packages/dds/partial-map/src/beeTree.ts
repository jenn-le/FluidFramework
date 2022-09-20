/*!
 * Copyright (c) Microsoft Corporation and contributors. All rights reserved.
 * Licensed under the MIT License.
 */

import { IFluidHandle } from "@fluidframework/core-interfaces";
import { Serializable } from "@fluidframework/datastore-definitions";
import { IBeeTree, IHandleProvider, IQueenBee } from "./interfaces";

export class BeeTree<T = Serializable> implements IBeeTree, IHandleProvider {
    private readonly map = new Map<string, T>();

	constructor(node: IQueenBee) { }

	async get(key: string): Promise<T | undefined> {
        return this.map.get(key);
	}

	async batchUpdate(updates: Map<string, T>, deletes: Set<string>): Promise<string[]> {
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

    getGcData(): IFluidHandle[] {
        throw new Error("Method not implemented.");
    }
}

// A node in a BeeTree
class Pollen {
    private readonly keys: string[] = [];
    private readonly children: Pollen[] = [];

    public constructor() {

    }
}
