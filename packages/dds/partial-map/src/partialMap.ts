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
        = new ChunkedBTree(
            btreeOrder,
            this.createHandle.bind(this),
            this.resolveHandle.bind(this));

    private readonly sequencedState: SequencedState<any>;

    private readonly pendingState = new PendingState<any>();

    // Handles to pass to the GC whitelist
    private gcWhiteList: string[] = [];

    private cacheSizeHint = 5000;
    private flushThreshold = 1000;

    private refSequenceNumberOfLastFlush = -1;

    private pendingFlushUpload: Promise<boolean> | undefined = undefined;

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
        super(id, runtime, attributes, "fluid_partial_map_");
        this.sequencedState = new SequencedState(this.cacheSizeHint, (evictionCountHint) => {
            if (this.isAttached()) {
                this.btree.evict(evictionCountHint);
            }
        });
        this.leaderTracker = new LeaderTracker(runtime);
        this.leaderTracker.on("promoted", () => this.tryStartCompaction());
    }

    private initializePersistedState(btree: IChunkedBTree<any, ISerializedHandle>): void {
        // If GC uses a blacklist, we need to go through the previous btree and GC all the blobs
        this.btree = btree;
        this.gcWhiteList = [];
    }

    /**
     * The number of key/value pairs stored in the map.
     */
    public get size() {
        throw new Error("Method not implemented");
    }

    /**
     * The number of entries retained in memory by the partial map.
     */
    public workingSetSize(): number {
        return Math.max(this.sequencedState.size, this.btree.workingSetSize(), this.pendingState.size);
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
            this.sequencedState.set(key, value, -1 /* disconnected, so no refSequenceNum */);
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
            this.sequencedState.delete(key, -1 /* disconnected, so no refSequenceNum */);
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
            this.initializePersistedState(new ChunkedBTree(
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

    public setFlushThreshold(modificationCount: number): void {
        this.flushThreshold = modificationCount;
    }

    public setCacheSizeHint(cacheSizeHint: number): void {
        this.cacheSizeHint = cacheSizeHint;
    }

    public async pendingFlushCompleted(): Promise<boolean> {
        return this.pendingFlushUpload ?? Promise.resolve(false);
    }

    private tryStartCompaction(): void {
        if (this.sequencedState.unflushedChangeCount > this.flushThreshold) {
            this.emit(SharedPartialMapEvents.StartFlush);
            this.pendingFlushUpload = this.compact();
        }
    }

    private async compact(): Promise<boolean> {
        assert(this.leaderTracker.isLeader(), "Non-leader should not evict cache.");
        const [updates, deletes] = this.sequencedState.getFlushableChanges();
        const refSequenceNumber = this.runtime.deltaManager.lastSequenceNumber;
        let persistedState: ISharedPartialMapSummary<ISerializedHandle>;
        try {
            persistedState = await this.flushToNewBtree(updates, deletes);
        } catch (error) {
            // TODO: logging
            this.pendingFlushUpload = undefined;
            return false;
        }

        const evictionOp: CompactionOp = {
            type: OpType.Compact,
            persistedState,
            refSequenceNumber,
        };

        this.submitLocalMessage(evictionOp);
        assert(this.pendingFlushUpload !== undefined, "Pending flush should exist if local flush upload was started.");
        this.pendingFlushUpload = undefined;
        return true;
    }

    private async flushToNewBtree(
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
        const [updates, deletes] = this.sequencedState.getFlushableChanges();
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
        const [updates, deletes] = this.sequencedState.getFlushableChanges();
        const hive = await this.flushToNewBtree(updates, deletes);
        return createSingleBlobSummary(snapshotFileName, this.serializer.stringify(hive, this.handle));
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
        this.initializePersistedState(
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
            // A split-brain scenario could result in multiple concurrent flushes being sent,
            // and we should only process the latest one.
            if (op.refSequenceNumber > this.refSequenceNumberOfLastFlush) {
                this.refSequenceNumberOfLastFlush = op.refSequenceNumber;
                this.sequencedState.flush(op.refSequenceNumber);
                const { root, gcWhiteList } = op.persistedState;
                // TODO: this could retain downloaded chunks that are the same
                this.btree = ChunkedBTree.loadSync(
                    root,
                    this.createHandle.bind(this),
                    this.resolveHandle.bind(this),
                    isSerializedHandle,
                );
                this.gcWhiteList = gcWhiteList;
                this.emit(SharedPartialMapEvents.Flush, this.leaderTracker.isLeader());
            }
        } else {
            switch (op.type) {
                case OpType.Set:
                    this.sequencedState.set(op.key, this.serializer.parse(op.value), message.sequenceNumber);
                    if (local) {
                        this.pendingState.ackModify(op.key);
                    } else {
                        this.emit(SharedPartialMapEvents.ValueChanged, op.key, local);
                    }
                    break;
                case OpType.Delete:
                    this.sequencedState.delete(op.key, message.sequenceNumber);
                    if (local) {
                        this.pendingState.ackModify(op.key);
                    } else {
                        this.emit(SharedPartialMapEvents.ValueChanged, op.key, local);
                    }
                    break;
                case OpType.Clear: {
                    this.initializePersistedState(new ChunkedBTree(
                        btreeOrder,
                        this.createHandle.bind(this),
                        this.resolveHandle.bind(this)),
                    );
                    this.sequencedState.clear();
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
