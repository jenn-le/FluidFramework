/*!
 * Copyright (c) Microsoft Corporation and contributors. All rights reserved.
 * Licensed under the MIT License.
 */

import { ISequencedDocumentMessage } from "@fluidframework/protocol-definitions";
import {
    IChannelAttributes,
    IFluidDataStoreRuntime,
    IChannelStorageService,
    IChannelServices,
    IChannelFactory,
    Serializable,
} from "@fluidframework/datastore-definitions";
import { IGarbageCollectionData, ISummaryTreeWithStats, ITelemetryContext } from "@fluidframework/runtime-definitions";
import {
    createSingleBlobSummary,
    IFluidSerializer,
    SharedObject,
} from "@fluidframework/shared-object-base";
import { pkgVersion } from "./packageVersion";
import { BeeTree } from "./beeTree";
import { IBeeTree, IHashcache, ISharedPartialMapEvents } from "./interfaces";
import { Hashcache } from "./hashcache";
import { ClearOp, DeleteOp, IHive, IQueenBee, OpType, PartialMapOp, SetOp } from "./persistedTypes";
import { BeeTreeJSMap } from "./beeTreeTemp";
import { readAndParse } from "@fluidframework/driver-utils";

// interface IMapSerializationFormat {
//     blobs?: string[];
//     content: IMapDataObjectSerializable;
// }

const snapshotFileName = "hive";

/**
 * {@link @fluidframework/datastore-definitions#IChannelFactory} for {@link SharedPartialMap}.
 *
 * @sealed
 */
export class PartialMapFactory implements IChannelFactory {
    /**
     * {@inheritDoc @fluidframework/datastore-definitions#IChannelFactory."type"}
     */
    public static readonly Type = "https://graph.microsoft.com/types/partial-map";

    /**
     * {@inheritDoc @fluidframework/datastore-definitions#IChannelFactory.attributes}
     */
    public static readonly Attributes: IChannelAttributes = {
        type: PartialMapFactory.Type,
        snapshotFormatVersion: "0.2",
        packageVersion: pkgVersion,
    };

    /**
     * {@inheritDoc @fluidframework/datastore-definitions#IChannelFactory."type"}
     */
    public get type() {
        return PartialMapFactory.Type;
    }

    /**
     * {@inheritDoc @fluidframework/datastore-definitions#IChannelFactory.attributes}
     */
    public get attributes() {
        return PartialMapFactory.Attributes;
    }

    /**
     * {@inheritDoc @fluidframework/datastore-definitions#IChannelFactory.load}
     */
    public async load(
        runtime: IFluidDataStoreRuntime,
        id: string,
        services: IChannelServices,
        attributes: IChannelAttributes): Promise<SharedPartialMap> {
        const map = new SharedPartialMap(id, runtime, attributes);
        await map.load(services);

        return map;
    }

    /**
     * {@inheritDoc @fluidframework/datastore-definitions#IChannelFactory.create}
     */
    public create(runtime: IFluidDataStoreRuntime, id: string): SharedPartialMap {
        return new SharedPartialMap(id, runtime, PartialMapFactory.Attributes);
    }
}

// For Noah <3
const ORDER = 32;

/**
 * {@inheritDoc ISharedPartialMap}
 */
export class SharedPartialMap extends SharedObject<ISharedPartialMapEvents> {
    /**
     * Create a new shared partial map.
     * @param runtime - The data store runtime that the new shared partial map belongs to.
     * @param id - Optional name of the shared partial map.
     * @returns Newly created shared partial map.
     *
     * @example
     * To create a `SharedPartialMap`, call the static create method:
     *
     * ```typescript
     * const myMap = SharedPartialMap.create(this.runtime, id);
     * ```
     */
    public static create(runtime: IFluidDataStoreRuntime, id?: string): SharedPartialMap {
        return runtime.createChannel(id, PartialMapFactory.Type) as SharedPartialMap;
    }

    /**
     * Get a factory for SharedPartialMap to register with the data store.
     * @returns A factory that creates SharedPartialMaps and loads them from storage.
     */
    public static getFactory(): IChannelFactory {
        return new PartialMapFactory();
    }

    /**
     * String representation for the class.
     */
    public readonly [Symbol.toStringTag]: string = "SharedPartialMap";

    private beeTree: IBeeTree<any> = undefined as unknown as IBeeTree<any>;
    private hashcache: IHashcache<any> = undefined as unknown as IHashcache<any>;

    // Handles to pass to the GC whitelist
    private readonly honeycombs = new Set<string>();

    /**
     * Keys that have been modified locally but not yet ack'd from the server.
     */
    private pendingKeys: Map<string, number> = new Map();
    private pendingClearCount = 0;

    /**
     * Do not call the constructor. Instead, you should use the {@link SharedPartialMap.create | create method}.
     *
     * @param id - String identifier.
     * @param runtime - Data store runtime.
     * @param attributes - The attributes for the map.
     */
    constructor(
        id: string,
        runtime: IFluidDataStoreRuntime,
        attributes: IChannelAttributes,
    ) {
        super(id, runtime, attributes, "fluid_map_");
        this.initializePartialMap(new BeeTreeJSMap());
    }

    private initializePartialMap(beeTree: IBeeTree<any>): void {
        // If GC uses a blacklist, we need to go through the previous beeTree and GC all the blobs
        this.beeTree = beeTree;
        this.hashcache = new Hashcache();

        this.beeTree.on("handleAdded", (handle) => {
            // TODO do we need to do anything?
            this.honeycombs.add(handle.absolutePath);
        });

        this.beeTree.on("handleRemoved", (handle) => {
            // TODO should we still use this for GC?
            this.honeycombs.delete(handle.absolutePath);
        });

        this.honeycombs.clear();

        this.pendingKeys = new Map();
    }

    /**
     * The number of key/value pairs stored in the map.
     * TODO
     */
    public get size() {
        return 0;
    }

    /**
     * {@inheritDoc ISharedPartialMap.get}
     */
    public async get<T = Serializable>(key: string): Promise<T | undefined> {
        if (!this.hashcache.has(key)) {
            const stored = await this.beeTree.get(key);

            if (stored !== undefined) {
                this.hashcache.set(key, stored);
            }

            return stored as T;
        }

        return this.hashcache.get(key) as T;
    }

    /**
     * Check if a key exists in the map.
     * @param key - The key to check
     * @returns True if the key exists, false otherwise
     */
    public async has(key: string): Promise<boolean> {
        // TODO: this.beeTree.has(key)
        return this.hashcache.has(key);
    }

    private incrementLocalKeyCount(key: string): void {
        this.adjustLocalKeyCount(key, true);
    }

    private decrementLocalKeyCount(key: string): void {
        this.adjustLocalKeyCount(key, false);
    }

    private adjustLocalKeyCount(key: string, isIncrement: boolean): void {
        let currentKeyCount = this.pendingKeys.get(key) ?? 0;
        if (isIncrement) {
            currentKeyCount++;
        } else {
            if (currentKeyCount === 0) {
                return;
            }

            currentKeyCount--;
        }
        this.pendingKeys.set(key, currentKeyCount);
    }

    /**
     * {@inheritDoc ISharedPartialMap.set}
     */
    public set(key: string, value: any): this {
        this.incrementLocalKeyCount(key);
        const op: SetOp = {
            type: OpType.Set,
            key,
            value,
        };
        this.submitLocalMessage(op);
        this.hashcache.set(key, value);
        return this;
    }

    /**
     * Delete a key from the map.
     * @param key - Key to delete
     * @returns True if the key existed and was deleted, false if it did not exist
     */
    public delete(key: string): boolean {
        this.incrementLocalKeyCount(key);
        const op: DeleteOp = {
            type: OpType.Delete,
            key,
        };
        this.submitLocalMessage(op);
        return this.hashcache.delete(key);
    }

    /**
     * Clear all data from the map.
     */
    public clear(): void {
        this.pendingClearCount++;
        const op: ClearOp = {
            type: OpType.Clear,
        };
        this.submitLocalMessage(op);
        this.initializePartialMap(new BeeTreeJSMap());
    }

    /**
     * {@inheritDoc @fluidframework/shared-object-base#SharedObject.summarizeCore}
     * @internal
     */
    protected summarizeCore(
        serializer: IFluidSerializer,
        telemetryContext?: ITelemetryContext,
    ): ISummaryTreeWithStats {
        throw new Error("SharedPartialMap");
    }

    override getAttachSummary(
        fullTree?: boolean | undefined,
        trackState?: boolean | undefined,
        telemetryContext?: ITelemetryContext | undefined,
    ): ISummaryTreeWithStats {
        throw new Error("implement me pls");
    }

    override async summarize(
        fullTree?: boolean | undefined,
        trackState?: boolean | undefined,
        telemetryContext?: ITelemetryContext | undefined,
    ): Promise<ISummaryTreeWithStats> {
        const [updates, deletes] = this.hashcache.flushUpdates();
        const queen = await this.beeTree.summarize(updates, deletes);

        const hive: IHive = {
            queen,
            honeycombs: Array.from(this.honeycombs.values()),
        };

        return createSingleBlobSummary(snapshotFileName, this.serializer.stringify(hive, this.handle));
    }

    public getGCData(fullGC?: boolean | undefined): IGarbageCollectionData {
        return { gcNodes: { "/": Array.from(this.honeycombs.values()) } };
    }

    /**
     * {@inheritDoc @fluidframework/shared-object-base#SharedObject.loadCore}
     * @internal
     */
    protected async loadCore(storage: IChannelStorageService) {
        const json = await readAndParse<object>(storage, snapshotFileName);
        const hive = json as IHive;
        this.initializePartialMap(await BeeTreeJSMap.create(hive.queen, this.serializer));
    }

    /**
     * {@inheritDoc @fluidframework/shared-object-base#SharedObject.onDisconnect}
     * @internal
     */
     protected onDisconnect() { }

    /**
     * {@inheritDoc @fluidframework/shared-object-base#SharedObject.reSubmitCore}
     * @internal
     */
    protected reSubmitCore(content: any, localOpMetadata: unknown) {
        throw new Error("Implement me");
    }

    /**
     * {@inheritDoc @fluidframework/shared-object-base#SharedObjectCore.applyStashedOp}
     * @internal
     */
    protected applyStashedOp(content: any): unknown {
        throw new Error("Implement me");
    }

    /**
     * {@inheritDoc @fluidframework/shared-object-base#SharedObject.processCore}
     * @internal
     */
    protected processCore(message: ISequencedDocumentMessage, local: boolean, localOpMetadata: unknown) {
        const op = message.contents as PartialMapOp;

        if (local) {
            if (op.type === OpType.Clear) {
                this.pendingClearCount--;
            } else {
                this.decrementLocalKeyCount(op.key);
            }
        } else {
            if (this.pendingClearCount !== 0) {
                switch (op.type) {
                    case OpType.Set:
                        if (!this.pendingKeys.has(op.key)) {
                            this.hashcache.set(op.key, op.value);
                        }
                        break;
                    case OpType.Delete:
                        if (!this.pendingKeys.has(op.key)) {
                            this.hashcache.delete(op.key);
                        }
                    case OpType.Clear:
                        this.initializePartialMap(new BeeTreeJSMap());
                    default:
                        throw new Error("Unsupported op type");
                }
            }
        }
    }

    // /**
    //  * {@inheritDoc @fluidframework/shared-object-base#SharedObject.rollback}
    //  * @internal
    // */
    // protected rollback(content: any, localOpMetadata: unknown) {
    //     this.kernel.rollback(content, localOpMetadata);
    // }
}
