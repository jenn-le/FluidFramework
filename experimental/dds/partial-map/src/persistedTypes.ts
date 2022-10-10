/*!
 * Copyright (c) Microsoft Corporation and contributors. All rights reserved.
 * Licensed under the MIT License.
 */

import { ISerializedHandle } from "@fluidframework/shared-object-base";

export interface ISharedPartialMapSummary<TRoot, THandle> {
    readonly btree: ISerializedBtree<TRoot, THandle>;
}

export interface IBtreeUpdate<THandle> {
    readonly newRoot: THandle;
    readonly newHandles: THandle[];
    readonly deletedHandles: THandle[];
}

export interface ISerializedBtree<TRoot, THandle> {
    readonly order: number;
    readonly root: TRoot;
    readonly handles: THandle[];
}

export interface IBtreeInteriorNode<THandle> {
    readonly keys: readonly string[];
    readonly children: readonly THandle[];
}

export interface IBtreeLeafNode {
    readonly keys: readonly string[];
    readonly values: readonly any[];
}

export enum OpType {
    Set,
    Delete,
    Clear,
    Flush,
}

export type PartialMapOp = SetOp | DeleteOp | ClearOp | FlushOp;

export interface SetOp {
    type: OpType.Set;
    key: string;
    value: any;
}

export interface DeleteOp {
    type: OpType.Delete;
    key: string;
}

export interface ClearOp {
    type: OpType.Clear;
}

export interface FlushOp {
    type: OpType.Flush;
    update: IBtreeUpdate<ISerializedHandle>;
    refSequenceNumber: number;
}
