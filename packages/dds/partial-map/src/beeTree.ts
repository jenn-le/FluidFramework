/*!
 * Copyright (c) Microsoft Corporation and contributors. All rights reserved.
 * Licensed under the MIT License.
 */

import { IFluidHandle } from "@fluidframework/core-interfaces";
import { Serializable } from "@fluidframework/datastore-definitions";
import { IBeeTree, IHandleProvider, IQueenBee } from "./interfaces";
import { Tombstone, tombstone } from "./common";

export class BeeTree<T = Serializable> implements IBeeTree, IHandleProvider {
    private readonly map = new Map<string, T>();

	constructor(node: IQueenBee) { }

	async get(key: string): Promise<T | undefined> {
        return this.map.get(key);
	}

	async batchUpdate(updates: Map<string, T | Tombstone>): Promise<Map<string, T | Tombstone>> {
        const failedUpdates = new Map<string, T | Tombstone>();

		for (const [key, value] of updates.entries()) {
            if (value === tombstone) {
                if (!this.map.delete(key)) {
                    failedUpdates.set(key, tombstone);
                }
            } else {
                if (!this.map.set(key, value)) {
                    failedUpdates.set(key, value);
                }
            }
        }

        return failedUpdates;
	}

    getGcData(): IFluidHandle[] {
        throw new Error("Method not implemented.");
    }
}
