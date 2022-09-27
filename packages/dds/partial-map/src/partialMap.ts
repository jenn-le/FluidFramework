/*!
 * Copyright (c) Microsoft Corporation and contributors. All rights reserved.
 * Licensed under the MIT License.
 */

import { fail } from "assert";
import { assert } from "console";
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
    isSerializedHandle,
    SharedObject,
} from "@fluidframework/shared-object-base";
import { readAndParse } from "@fluidframework/driver-utils";
import { bufferToString, stringToBuffer } from "@fluidframework/common-utils";
import { IFluidHandle } from "@fluidframework/core-interfaces";
import { pkgVersion } from "./packageVersion";
import { IChunkedBTree, ISharedPartialMapEvents, SharedPartialMapEvents } from "./interfaces";
import { SequencedState } from "./sequencedState";
import {
    ClearOp,
    DeleteOp,
    CompactionOp,
    IBtreeLeafNode,
    ISharedPartialMapSummary,
    IBtreeInteriorNode,
    OpType,
    PartialMapOp,
    SetOp,
} from "./persistedTypes";
import { ChunkedBTree } from "./chunkedBTree";
import { LeaderTracker } from "./leaderTracker";
import { PendingState } from "./pendingState";

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

const btreeOrder = 32;
const cacheSizeHint = 5000;
const changeCountToFlushAt = 100;

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

    private readonly leaderTracker: LeaderTracker;

    private btree: IChunkedBTree<any, ISerializedHandle>
        = undefined as unknown as IChunkedBTree<any, ISerializedHandle>;

    private sequencedState: SequencedState<any> = new SequencedState(cacheSizeHint);

    // Handles to pass to the GC whitelist
    private gcWhiteList: string[] = [];

    private readonly pendingState = new PendingState<any>();

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
        this.leaderTracker = new LeaderTracker(runtime);
        this.initializePartialMap(new ChunkedBTree(
            btreeOrder,
            this.createHandle.bind(this),
            this.resolveHandle.bind(this)),
        );
        this.leaderTracker.on("promoted", () => this.tryStartCompaction());
    }

    private initializePartialMap(btree: IChunkedBTree<any, ISerializedHandle>): void {
        // If GC uses a blacklist, we need to go through the previous btree and GC all the blobs
        this.btree = btree;
        this.sequencedState = new SequencedState(cacheSizeHint);
        this.gcWhiteList = [];
    }

    /**
     * The number of key/value pairs stored in the map.
     * TODO
     */
    public get size() {
        throw new Error("Method not implemented");
    }

    /**
     * Read a key/value in the map.
     */
    public async get<T = Serializable>(key: string): Promise<T | undefined> {
        const { value: pendingValue, keyIsModified: pendingKeyIsModified } = this.pendingState.get(key);
        if (pendingKeyIsModified) {
            return pendingValue as T;
        }

        const { value: cacheValue, keyIsModified: cacheKeyIsModified } = this.sequencedState.get(key);
        if (cacheKeyIsModified) {
            return cacheValue as T;
        }

        const stored = await this.btree.get(key);

        if (stored !== undefined) {
            this.sequencedState.cache(key, stored);
        }

        return stored as T;
    }

    /**
     * Check if a key exists in the map.
     * @param key - The key to check
     * @returns True if the key exists, false otherwise
     */
    public async has(key: string): Promise<boolean> {
        const { keyIsModified: pendingKeyIsModified, isDeleted: pendingIsDeleted } = this.pendingState.get(key);
        if (pendingKeyIsModified) {
            return !pendingIsDeleted;
        }

        const { keyIsModified: cacheKeyIsModified, isDeleted: cacheIsDeleted } = this.sequencedState.get(key);
        if (cacheKeyIsModified) {
            return !cacheIsDeleted;
        }

        return this.btree.has(key);
    }

    /**
     * Sets the value stored at key to the provided value.
     * @param key - Key to set
     * @param value - Value to set
     * @returns The {@link ISharedMap} itself
     */
    public set(key: string, value: any): this {
        // Undefined/null keys can't be serialized to JSON in the manner we currently snapshot.
        if (key === undefined || key === null) {
            throw new Error("Undefined and null keys are not supported");
        }

        if (this.isAttached()) {
            this.pendingState.set(key, value);
        } else {
            // Emulate an immediate ack
            this.sequencedState.set(key, value);
        }

        this.emit(SharedPartialMapEvents.ValueChanged, key, true);

        // If we are not attached, don't submit the op.
        if (!this.isAttached()) {
            return this;
        }

        const opValue = this.serializer.stringify(
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
    public delete(key: string): void {
        if (this.isAttached()) {
            this.pendingState.delete(key);
        } else {
            // Emulate an immediate ack
            this.sequencedState.delete(key);
        }

        this.emit(SharedPartialMapEvents.ValueChanged, key, true);

        // If we are not attached, don't submit the op.
        if (!this.isAttached()) {
            return;
        }

        const op: DeleteOp = {
            type: OpType.Delete,
            key,
        };
        this.submitLocalMessage(op);
    }

    /**
     * Clear all data from the map.
     */
    public clear(): void {
        if (this.isAttached()) {
            this.pendingState.clear();
        } else {
            // Emulate an immediate ack
            this.initializePartialMap(new ChunkedBTree(
                btreeOrder,
                this.createHandle.bind(this),
                this.resolveHandle.bind(this)),
            );
        }

        this.emit(SharedPartialMapEvents.Clear, true);

        // If we are not attached, don't submit the op.
        if (!this.isAttached()) {
            return;
        }

        const op: ClearOp = {
            type: OpType.Clear,
        };
        this.submitLocalMessage(op);
    }

    private tryStartCompaction(): void {
        if (this.sequencedState.unflushedChangeCount > changeCountToFlushAt) {
            this.compact().catch((reason) => {});
        }
    }

    private async compact(): Promise<void> {
        assert(this.leaderTracker.isLeader(), "Non-leader should not evict cache.");
        const [updates, deletes] = this.sequencedState.startFlush();
        const hive = await this.updateHive(updates, deletes);

        const evictionOp: CompactionOp = {
            type: OpType.Compact,
            hive,
        };

        this.submitLocalMessage(evictionOp);
        this.sequencedState.endFlush();
    }

    private async updateHive(
        updates: Map<string, any>, deletes: Set<string>): Promise<ISharedPartialMapSummary<ISerializedHandle>> {
        const queen = await this.btree.flush(updates, deletes);

        const hive: ISharedPartialMapSummary<ISerializedHandle> = {
            root: queen,
            gcWhiteList: this.gcWhiteList,
        };
        return hive;
    }

    /**
     * {@inheritDoc @fluidframework/shared-object-base#SharedObject.summarizeCore}
     * @internal
     */
    protected summarizeCore(
        serializer: IFluidSerializer,
        telemetryContext?: ITelemetryContext,
    ): ISummaryTreeWithStats {
        throw new Error("Summarization is overridden.");
    }

    override getAttachSummary(
        fullTree?: boolean | undefined,
        trackState?: boolean | undefined,
        telemetryContext?: ITelemetryContext | undefined,
    ): ISummaryTreeWithStats {
        assert(this.runtime.deltaManager.lastKnownSeqNumber === 0, "No ops should be processed before attachment.");
        const [updates, deletes] = this.sequencedState.startFlush();
        const root = this.btree.flushSync(updates, deletes);

        const summary: ISharedPartialMapSummary<IBtreeLeafNode> = {
            root,
            gcWhiteList: this.gcWhiteList,
        };

        return createSingleBlobSummary(snapshotFileName, this.serializer.stringify(summary, this.handle));
    }

    override async summarize(
        fullTree?: boolean | undefined,
        trackState?: boolean | undefined,
        telemetryContext?: ITelemetryContext | undefined,
    ): Promise<ISummaryTreeWithStats> {
        const [updates, deletes] = this.sequencedState.startFlush();
        const hive = await this.updateHive(updates, deletes);
        const summary = createSingleBlobSummary(snapshotFileName, this.serializer.stringify(hive, this.handle));
        this.sequencedState.endFlush();
        return summary;
    }

    private async createHandle(
        content: IBtreeInteriorNode<ISerializedHandle> | IBtreeLeafNode): Promise<ISerializedHandle> {
        const serializedContents = this.serializer.stringify(content, this.handle);
        const buffer = stringToBuffer(serializedContents, "utf-8");
        const editHandle = await this.runtime.uploadBlob(buffer);
        const serialized: ISerializedHandle = this.serializer.encode(editHandle, this.handle) ??
            fail("Edit chunk handle could not be serialized.");

        return serialized;
    }

    private async resolveHandle(
        handle: ISerializedHandle | IBtreeLeafNode,
    ): Promise<IBtreeInteriorNode<ISerializedHandle> | IBtreeLeafNode> {
        const editHandle: IFluidHandle<ArrayBufferLike> = this.serializer.decode(handle);
        const serializedContents = bufferToString(await editHandle.get(), "utf-8");
        const node: IBtreeInteriorNode<ISerializedHandle> | IBtreeLeafNode = this.serializer.parse(serializedContents);
        return node;
    }

    public getGCData(fullGC?: boolean | undefined): IGarbageCollectionData {
        // TODO: don't use blob manager, then this method becomes a noop
        return { gcNodes: { "/": this.gcWhiteList } };
    }

    /**
     * {@inheritDoc @fluidframework/shared-object-base#SharedObject.loadCore}
     * @internal
     */
    protected async loadCore(storage: IChannelStorageService) {
        const json = await readAndParse<object>(storage, snapshotFileName);
        const hive = json as ISharedPartialMapSummary<ISerializedHandle | IBtreeLeafNode>;
        this.initializePartialMap(
            await ChunkedBTree.load(
                hive.root,
                this.createHandle.bind(this),
                this.resolveHandle.bind(this),
                isSerializedHandle,
            ),
        );
    }

    /**
     * {@inheritDoc @fluidframework/shared-object-base#SharedObject.onDisconnect}
     * @internal
     */
     protected onDisconnect() { }

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

        if (op.type === OpType.Compact) {
            if (local) {
                this.sequencedState.endFlush();
            }
            const { root, gcWhiteList } = op.hive;
            // TODO: this could retain downloaded chunks that are the same
            this.btree = ChunkedBTree.loadSync(
                root,
                this.createHandle.bind(this),
                this.resolveHandle.bind(this),
                isSerializedHandle,
            );
            this.gcWhiteList = gcWhiteList;
        } else {
            switch (op.type) {
                case OpType.Set:
                    this.sequencedState.set(op.key, this.serializer.parse(op.value));
                    if (local) {
                        this.pendingState.ackModify(op.key);
                    } else {
                        this.emit(SharedPartialMapEvents.ValueChanged, op.key, local);
                    }
                    this.emit(SharedPartialMapEvents.ValueChanged, op.key, local);
                    break;
                case OpType.Delete:
                    this.sequencedState.delete(op.key);
                    if (local) {
                        this.pendingState.ackModify(op.key);
                    } else {
                        this.emit(SharedPartialMapEvents.ValueChanged, op.key, local);
                    }
                    break;
                case OpType.Clear: {
                    this.initializePartialMap(new ChunkedBTree(
                        btreeOrder,
                        this.createHandle.bind(this),
                        this.resolveHandle.bind(this)),
                    );
                    if (local) {
                        this.pendingState.ackClear();
                    } else {
                        this.emit(SharedPartialMapEvents.Clear, local);
                    }
                    break;
                }
                default:
                    throw new Error("Unsupported op type");
            }
            if (this.leaderTracker.isLeader()) {
                this.tryStartCompaction();
            }
        }
    }
}
