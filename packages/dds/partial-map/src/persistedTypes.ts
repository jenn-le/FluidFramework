/*!
 * Copyright (c) Microsoft Corporation and contributors. All rights reserved.
 * Licensed under the MIT License.
 */

import { ISerializedHandle } from "@fluidframework/shared-object-base";

export interface IHive {
    readonly queen: IQueenBee;
    readonly honeycombs: Honeycombs;
}

export interface IQueenBee {
    readonly order: number;
    readonly root: ISerializedHandle;
}

export interface IWorkerBee {
    readonly keys: readonly string[];
    readonly children: readonly ISerializedHandle[];
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
