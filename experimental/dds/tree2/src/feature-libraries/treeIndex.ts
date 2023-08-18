/*!
 * Copyright (c) Microsoft Corporation and contributors. All rights reserved.
 * Licensed under the MIT License.
 */

import { Delta, FieldKey } from "../core";
import { Brand, SizedNestedMap, brand } from "../util";

/**
 * ID used to create a detached field key for a removed subtree.
 * @alpha
 */
export type RemovedTreeId = Brand<string, "tree.RemovedTreeId">;

/**
 * The tree index records detached field ids and associates them with a change atom ID.
 */
export class TreeIndex {
	private readonly detachedFields = new SizedNestedMap<
		string | number | undefined,
		string | number | undefined,
		FieldKey
	>();

	public constructor(private readonly name: string) {}

	/**
	 * Returns a field key for the given ID. Should be unique for this index.
	 * This does not save the field key on the index. To do so, call {@link setFieldKey}.
	 */
	public getFieldKey(id: RemovedTreeId): FieldKey {
		return brand(`${this.name}-${id}`);
	}

	/**
	 * Associates the change atom ID with the field key on this index.
	 */
	public setFieldKey(changeAtomId: Delta.RemovedNodeId, fieldKey: FieldKey): void {
		const { major, minor } = changeAtomId;
		this.detachedFields.set(major, minor, fieldKey);
	}
}
