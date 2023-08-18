/*!
 * Copyright (c) Microsoft Corporation and contributors. All rights reserved.
 * Licensed under the MIT License.
 */

import { Delta, FieldKey } from "../core";
import {
	Brand,
	NestedMap,
	brand,
	getOrDefaultInNestedMap,
	setInNestedMap,
	tryGetFromNestedMap,
} from "../util";

/**
 * ID used to create a detached field key for a removed subtree.
 * @alpha
 *
 * TODO: Move to Forest once forests can support multiple roots.
 */
export type ForestRootId = Brand<number, "tree.ForestRootId">;

type RemovedNodeKey = string | number | undefined;

/**
 * The tree index records detached field ids and associates them with a change atom ID.
 */
export class TreeIndex {
	private readonly detachedFields: NestedMap<RemovedNodeKey, RemovedNodeKey, FieldKey> = new Map<
		RemovedNodeKey,
		Map<RemovedNodeKey, FieldKey>
	>();

	public constructor(private readonly name: string) {}

	/**
	 * Returns a field key for the given ID.
	 * This does not save the field key on the index. To do so, call {@link createEntry}.
	 */
	private toFieldKey(id: ForestRootId): FieldKey {
		return brand(`${this.name}-${id}`);
	}

	/**
	 * Returns the FieldKey associated with the given id.
	 * Returns undefined if no such id is known to the index.
	 */
	public tryGetFieldKey(id: Delta.RemovedNodeId): FieldKey | undefined {
		return tryGetFromNestedMap(this.detachedFields, id.major, id.minor);
	}

	/**
	 * Associates the RemovedNodeId with a field key and creates an entry for it in the index.
	 */
	public createEntry(nodeId: Delta.RemovedNodeId, id: ForestRootId): FieldKey {
		const { major, minor } = nodeId;
		const fieldKey = this.toFieldKey(id);
		setInNestedMap(this.detachedFields, major, minor, fieldKey);
		return fieldKey;
	}
}
