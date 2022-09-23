/*!
 * Copyright (c) Microsoft Corporation and contributors. All rights reserved.
 * Licensed under the MIT License.
 */

import { fail } from "assert";
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
    ISerializedHandle,
    SharedObject,
} from "@fluidframework/shared-object-base";
import { readAndParse } from "@fluidframework/driver-utils";
import { IsoBuffer } from "@fluidframework/common-utils";
import { pkgVersion } from "./packageVersion";
import { IBeeTree, IHashcache, ISharedPartialMapEvents, SharedPartialMapEvents } from "./interfaces";
import { Hashcache } from "./hashcache";
import { ClearOp, DeleteOp, IHive, OpType, PartialMapOp, SetOp } from "./persistedTypes";
import { BeeTreeJSMap } from "./beeTreeTemp";

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
// const ORDER = 32;

/**
 *
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

    private beeTree: IBeeTree<any, ISerializedHandle> = undefined as unknown as IBeeTree<any, ISerializedHandle>;
    private hashcache: IHashcache<any> = undefined as unknown as IHashcache<any>;

    // Handles to pass to the GC whitelist
    private readonly honeycombs = new Set<string>();

    /**
     * Keys that have been modified locally but not yet ack'd from the server.
     */
    private readonly pendingKeys: Map<string, number> = new Map();
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

    private initializePartialMap(beeTree: IBeeTree<any, ISerializedHandle>): void {
        // If GC uses a blacklist, we need to go through the previous beeTree and GC all the blobs
        this.beeTree = beeTree;
        this.hashcache = new Hashcache();
        this.honeycombs.clear();
    }

    /**
     * The number of key/value pairs stored in the map.
     * TODO
     */
    public get size() {
        return 0;
    }

    /**
     *
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
            this.pendingKeys.set(key, currentKeyCount);
        } else {
            if (currentKeyCount === 0) {
                fail("bad");
            }

            currentKeyCount--;

            if (currentKeyCount === 0) {
                this.pendingKeys.delete(key);
            } else {
                this.pendingKeys.set(key, currentKeyCount);
            }
        }
    }

    /**
     *
     */
    public set(key: string, value: any): this {
        // Undefined/null keys can't be serialized to JSON in the manner we currently snapshot.
        if (key === undefined || key === null) {
            throw new Error("Undefined and null keys are not supported");
        }

        this.hashcache.set(key, value);
        this.emit(SharedPartialMapEvents.ValueChanged, key, true);

        // If we are not attached, don't submit the op.
        if (!this.isAttached()) {
            return this;
        }

        this.incrementLocalKeyCount(key);
        const opValue = this.serializer.encode(
            value,
            this.handle);
        const op: SetOp = {
            type: OpType.Set,
            key,
            value: opValue,
        };
        this.submitLocalMessage(op);
        return this;
    }

    /**
     * Delete a key from the map.
     * @param key - Key to delete
     * @returns True if the key existed and was deleted, false if it did not exist
     */
    public delete(key: string): boolean {
        const result = this.hashcache.delete(key);
        this.emit(SharedPartialMapEvents.ValueChanged, key, true);

        // If we are not attached, don't submit the op.
        if (!this.isAttached()) {
            return result;
        }

        this.incrementLocalKeyCount(key);
        const op: DeleteOp = {
            type: OpType.Delete,
            key,
        };
        this.submitLocalMessage(op);
        return result;
    }

    /**
     * Clear all data from the map.
     */
    public clear(): void {
        this.initializePartialMap(new BeeTreeJSMap());
        this.emit(SharedPartialMapEvents.Clear, true);

        // If we are not attached, don't submit the op.
        if (!this.isAttached()) {
            return;
        }

        this.pendingClearCount++;
        const op: ClearOp = {
            type: OpType.Clear,
        };
        this.submitLocalMessage(op);
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
        const [updates, deletes] = this.hashcache.flushUpdates();
        const queen = this.beeTree.summarizeSync(updates, deletes);

        const hive = {
            queen,
            honeycombs: Array.from(this.honeycombs.values()),
        };

        return createSingleBlobSummary(snapshotFileName, this.serializer.stringify(hive, this.handle));
    }

    override async summarize(
        fullTree?: boolean | undefined,
        trackState?: boolean | undefined,
        telemetryContext?: ITelemetryContext | undefined,
    ): Promise<ISummaryTreeWithStats> {
        const [updates, deletes] = this.hashcache.flushUpdates();
        const queen = await this.beeTree.summarize(
            updates,
            deletes,
            async (data: any) => {
                const serializedContents = this.serializer.encode(data, this.handle);
                const buffer = IsoBuffer.from(serializedContents);
                const editHandle = await this.runtime.uploadBlob(buffer);
                const serialized: ISerializedHandle = this.serializer.encode(editHandle, this.handle) ??
                    fail("Edit chunk handle could not be serialized.");

                return serialized;
            },
        );

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
            if (this.pendingClearCount === 0) {
                switch (op.type) {
                    case OpType.Set:
                        if (!this.pendingKeys.has(op.key)) {
                            this.hashcache.set(op.key, op.value);
                            this.emit(SharedPartialMapEvents.ValueChanged, local);
                        }
                        break;
                    case OpType.Delete:
                        if (!this.pendingKeys.has(op.key)) {
                            this.hashcache.delete(op.key);
                            this.emit(SharedPartialMapEvents.ValueChanged, local);
                        }
                        break;
                    case OpType.Clear: {
                        const oldHashcache = this.hashcache;
                        this.initializePartialMap(new BeeTreeJSMap());
                        for (const key of this.pendingKeys.keys()) {
                            this.hashcache.set(
                                key,
                                oldHashcache.get(key) ?? fail("Value should be set in the old cache"),
                            );
                        }
                        this.emit(SharedPartialMapEvents.Clear, local);
                        break;
                    }
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
