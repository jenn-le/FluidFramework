/*!
 * Copyright (c) Microsoft Corporation and contributors. All rights reserved.
 * Licensed under the MIT License.
 */

import { IEventThisPlaceHolder } from "@fluidframework/common-definitions";
import { IFluidHandle } from "@fluidframework/core-interfaces";
import { Serializable } from "@fluidframework/datastore-definitions";
import { ISharedObjectEvents } from "@fluidframework/shared-object-base";
import { Tombstone } from "./common";

/**
 * Type of "valueChanged" event parameter.
 */
 export interface IValueChanged {
    /**
     * The key storing the value that changed.
     */
    key: string;

    /**
     * The value that was stored at the key prior to the change.
     */
    previousValue: any;
}

/**
 * Events emitted in response to changes to the {@link ISharedMap | map} data.
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
     * - `target` - The {@link ISharedMap} itself.
     */
    (event: "valueChanged", listener: (
        changed: IValueChanged,
        local: boolean,
        target: IEventThisPlaceHolder) => void);

    /**
     * Emitted when the map is cleared.
     *
     * @remarks Listener parameters:
     *
     * - `local` - Whether the clear originated from this client.
     *
     * - `target` - The {@link ISharedMap} itself.
     */
    (event: "clear", listener: (
        local: boolean,
        target: IEventThisPlaceHolder) => void);
}

/**
 * TODO doc
 */
export interface IBeeTree<T> {
    get(key: string): Promise<T | undefined>;
    has(key: string): Promise<boolean>;
    batchUpdate(updates: Map<string, T | Tombstone>): Promise< Map<string, T | Tombstone>>;
}

export interface IHandleProvider {
    getGcData(): IFluidHandle[];
}

// eslint-disable-next-line @typescript-eslint/no-empty-interface
export interface IQueenBee {

}

/**
 * TODO doc
 */
export interface IHashbrown<T = Serializable> {
    get(key: string): T | Tombstone | undefined;
    has(key: string): boolean;
    set(key: string, value: T): void;
    delete(key: string): boolean;
}
