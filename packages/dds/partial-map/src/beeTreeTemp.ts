/* eslint-disable no-bitwise */
/*!
 * Copyright (c) Microsoft Corporation and contributors. All rights reserved.
 * Licensed under the MIT License.
 */

import { assert, TypedEventEmitter } from "@fluidframework/common-utils";
import { IBeeTree, IBeeTreeEvents, IHandleProvider } from "./interfaces";
import { IQueenBee } from "./persistedTypes";

export class BeeTreeJSMap<T> extends TypedEventEmitter<IBeeTreeEvents> implements IBeeTree<T> {
	public static async create(queen: IQueenBee, serializer: IFluidSerializer): Promise<BeeTreeJSMap<T>> {
        const deserializedHandle = serializer.parse(queen.root);
		assert(typeof deserializeHandle === 'object');

        // deserializedHandle

        // const

        return new BeeTreeJSMap();

	}

    private readonly map = new Map<string, T>();

	public async get(key: string): Promise<T | undefined> {
        return this.map.get(key);
	}

    public async has(key: string): Promise<boolean> {
        return this.map.has(key);
    }

	public async summarize(updates: Map<string, T>, deletes: Set<string>): Promise<IQueenBee> {
		for (const [key, value] of updates.entries()) {
			this.map.set(key, value);
        }

        for (const key of deletes.keys()) {
            this.delete(key);
        }

		const drone: IDroneBee = {
            keys: Array.from(this.map.keys()),
            values: Array.from(this.map.values()),
        };

        return queen;
	}
}
