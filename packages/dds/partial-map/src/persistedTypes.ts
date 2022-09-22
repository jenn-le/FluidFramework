/*!
 * Copyright (c) Microsoft Corporation and contributors. All rights reserved.
 * Licensed under the MIT License.
 */

import { ISerializedHandle } from "@fluidframework/shared-object-base";

export interface IHive {
    readonly queen: IQueenBee<ISerializedHandle>;
    readonly honeycombs: Honeycombs;
}

export interface IQueenBee<THandle> {
    readonly order: number;
    readonly root: THandle;
}

export interface IWorkerBee<THandle> {
    readonly keys: readonly string[];
    readonly children: readonly THandle[];
}

export interface IDroneBee {
    readonly keys: readonly string[];
    readonly values: readonly any[];
}

export type Honeycombs = string[];

export enum OpType {
    Set,
    Delete,
    Clear,
}

export type PartialMapOp = SetOp | DeleteOp | ClearOp;

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
