/*!
 * Copyright (c) Microsoft Corporation and contributors. All rights reserved.
 * Licensed under the MIT License.
 */

import { ISerializedHandle } from "@fluidframework/shared-object-base";

/**
 * Serialization format used for hydrating the hive
 */
export interface IQueenBee {
    readonly keys: readonly string[];
    readonly children: readonly ISerializedHandle[];
}

export interface IWorkerBee {
    readonly keys: readonly string[];
    readonly children: readonly (IWorkerBee | ISerializedHandle)[];
}

export interface IDroneBee {
    readonly keys: readonly string[];
    readonly values: readonly any[];
}

export type GcWhitelist = string[];
