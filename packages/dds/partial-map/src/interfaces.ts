/*!
 * Copyright (c) Microsoft Corporation and contributors. All rights reserved.
 * Licensed under the MIT License.
 */

import { ISharedObjectEvents } from "@fluidframework/shared-object-base";
import { IBtreeLeafNode, ISerializedBtree } from "./persistedTypes";

/**
 * Type of "valueChanged" event parameter.
 */
 export interface IValueChanged {
    /**
     * The key storing the value that changed.
     */
    key: string;
}

export enum SharedPartialMapEvents {
    ValueChanged = "valueChanged",
    Clear = "clear",
}

/**
 * Events emitted in response to changes to the  data.
 */
 export interface ISharedPartialMapEvents extends ISharedObjectEvents {
    /**
     * Emitted when a key is set or deleted.
     *
     * @remarks Listener parameters:
     *
     * - `changed` - Information on the key that changed and its value prior to the change.
     *
     * - `local` - Whether the change originated from this client.
     *
     * - `target` - The  itself.
     */
    (event: SharedPartialMapEvents.ValueChanged, listener: (changed: string) => void);

    /**
     * Emitted when the map is cleared.
     *
     * @remarks Listener parameters:
     *
     * - `local` - Whether the clear originated from this client.
     *
     * - `target` -  itself.
     */
    (event: SharedPartialMapEvents.Clear, listener: (local: boolean) => void);
}
/**
 * TODO doc
 */
export interface IChunkedBTree<T, THandle> {
    get(key: string): Promise<T | undefined>;
    has(key: string): Promise<boolean>;
    flush(
        updates: Map<string, T>,
        deletes: Set<string>,
    ): Promise<ISerializedBtree<THandle>>;
    flushSync(
        updates: Map<string, T>,
        deletes: Set<string>,
    ): ISerializedBtree<IBtreeLeafNode>;
}
