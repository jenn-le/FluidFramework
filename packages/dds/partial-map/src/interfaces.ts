/*!
 * Copyright (c) Microsoft Corporation and contributors. All rights reserved.
 * Licensed under the MIT License.
 */

import { Serializable } from "@fluidframework/datastore-definitions";
import { ISharedObjectEvents } from "@fluidframework/shared-object-base";
import { IDroneBee, IQueenBee } from "./persistedTypes";

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
export interface IBeeTree<T, THandle> {
    get(key: string): Promise<T | undefined>;
    has(key: string): Promise<boolean>;
    summarize(
        updates: Iterable<[string, T]>,
        deletes: Iterable<string>,
        uploadBlob: (data: any) => Promise<THandle>,
    ): Promise<IQueenBee<THandle>>;
    summarizeSync(
        updates: Iterable<[string, T]>,
        deletes: Iterable<string>,
    ): IQueenBee<IDroneBee>;
}

export interface IHandleProvider {
    getGcWhitelist(): string[];
}

/**
 * TODO doc
 */
export interface IHashcache<T = Serializable> {
    get(key: string): T | undefined;
    has(key: string): boolean;
    set(key: string, value: T): void;
    delete(key: string): boolean;
    flushUpdates(): [Map<string, T>, Set<string>];
}
