/*!
 * Copyright (c) Microsoft Corporation and contributors. All rights reserved.
 * Licensed under the MIT License.
 */

import { ISerializedHandle } from "@fluidframework/shared-object-base";

export interface ISharedPartialMapSummary<THandle> {
    readonly root: ISerializedBtree<THandle>;
    readonly gcWhiteList: GCWhiteList;
}

export interface ISerializedBtree<THandle> {
    readonly order: number;
    readonly root: THandle;
}

export interface IBtreeInteriorNode<THandle> {
    readonly keys: readonly string[];
    readonly children: readonly THandle[];
}

export interface IBtreeLeafNode {
    readonly keys: readonly string[];
    readonly values: readonly any[];
}

export type GCWhiteList = string[];

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
    persistedState: ISharedPartialMapSummary<ISerializedHandle>;
    refSequenceNumber: number;
}
