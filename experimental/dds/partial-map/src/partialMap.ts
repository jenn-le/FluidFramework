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
import { readAndParse } from "@fluidframework/driver-utils";
import { assert, bufferToString, stringToBuffer } from "@fluidframework/common-utils";
import { IFluidHandle } from "@fluidframework/core-interfaces";
import { pkgVersion } from "./packageVersion";
import { IBtreeState, IChunkedBtree, ISharedPartialMapEvents, SharedPartialMapEvents } from "./interfaces";
import { SequencedState } from "./sequencedState";
import {
    ClearOp,
    DeleteOp,
    FlushOp,
    IBtreeLeafNode,
    IBtreeInteriorNode,
    OpType,
    PartialMapOp,
    SetOp,
} from "./persistedTypes";
import { ChunkedBtree, Handler } from "./chunkedBTree";
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
const cacheSizeHintDefault = 5000;

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

    private btree: IChunkedBtree<any, IFluidHandle<ArrayBufferLike>, IFluidHandle>
        = ChunkedBtree.create(
            btreeOrder,
            this.createHandler(),
        );

    private readonly sequencedState: SequencedState<any>;

    private readonly pendingState = new PendingState<any>();

    private flushThreshold = 1000;

    private refSequenceNumberOfLastFlush = -1;

    // No pending flush | pending upload | awaiting flush ack
    private pendingFlush: undefined | Promise<boolean> | null = undefined;

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
        this.sequencedState = new SequencedState(cacheSizeHintDefault, (evictionCountHint) => {
            if (this.isAttached()) {
                this.btree.evict(evictionCountHint);
            }
        });
        this.leaderTracker = new LeaderTracker(runtime);
        this.leaderTracker.on("promoted", () => this.tryStartFlush());
    }

    /**
     * The number of entries retained in memory by the partial map.
     */
    public workingSetSize(): number {
        return Math.max(this.sequencedState.size, this.btree.workingSetSize()) + this.pendingState.size;
    }

    public get storageBtreeOrder(): number {
        return btreeOrder;
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
            this.sequencedState.cache(key, stored, this.workingSetSize());
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
            this.sequencedState.evict(this.workingSetSize());
        } else {
            // Emulate an immediate ack
            this.sequencedState.set(key, value, -1 /* disconnected, so no refSequenceNum */);
        }

        this.emit(SharedPartialMapEvents.ValueChanged, key, true);

        // If we are not attached, don't submit the op.
        if (!this.isAttached()) {
            return this;
        }

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
            this.btree = this.btree.clear();
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
        this.sequencedState.setCacheSizeHint(cacheSizeHint);
    }

    public async pendingFlushCompleted(): Promise<boolean> {
        return this.pendingFlush ?? Promise.resolve(false);
    }

    private tryStartFlush(): void {
        if (this.pendingFlush === undefined
            && this.sequencedState.unflushedChangeCount > this.flushThreshold) {
            this.emit(SharedPartialMapEvents.StartFlush);
            this.pendingFlush = this.flush();
        }
    }

    private async flush(): Promise<boolean> {
        assert(this.leaderTracker.isLeader(), "Non-leader should not evict cache.");
        const [updates, deletes] = this.sequencedState.getFlushableChanges();
        const refSequenceNumber = this.runtime.deltaManager.lastSequenceNumber;
        let newRoot: IFluidHandle<ArrayBufferLike>;
        let newHandles: (IFluidHandle<ArrayBufferLike> | IFluidHandle)[];
        let deletedHandles: (IFluidHandle<ArrayBufferLike> | IFluidHandle)[];
        try {
            ({ newRoot, newHandles, deletedHandles } = await this.btree.flush(updates, deletes));
        } catch (error) {
            // TODO: logging
            this.pendingFlush = undefined;
            return false;
        }

        const evictionOp: FlushOp = {
            type: OpType.Flush,
            update: this.serializer.encode({
                newRoot,
                newHandles,
                deletedHandles,
            }, this.handle),
            refSequenceNumber,
        };

        this.submitLocalMessage(evictionOp);
        assert(this.pendingFlush !== undefined && this.pendingFlush !== null,
            "Pending flush should exist if local flush upload was started.");
        this.pendingFlush = null;
        return true;
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
        const root = this.btree.summarizeSync(updates, deletes);
        const serializedRoot = this.serializer.stringify(root, this.handle);
        return createSingleBlobSummary(snapshotFileName, serializedRoot);
    }

    override async summarize(
        fullTree?: boolean | undefined,
        trackState?: boolean | undefined,
        telemetryContext?: ITelemetryContext | undefined,
    ): Promise<ISummaryTreeWithStats> {
        const [updates, deletes] = this.sequencedState.getFlushableChanges();
        const update = await this.btree.flush(updates, deletes);
        const newBtree = this.btree.update(update);
        const summary = { root: update.newRoot, order: this.btree.order, handles: newBtree.getAllHandles() };
        return createSingleBlobSummary(snapshotFileName, this.serializer.stringify(summary, this.handle));
    }

    private createHandler(): Handler<any, IFluidHandle<ArrayBufferLike>, IFluidHandle> {
        return {
            createHandle: async (content: IBtreeInteriorNode<IFluidHandle<ArrayBufferLike>> | IBtreeLeafNode) => {
                const serializedContents = this.serializer.stringify(content, this.handle);
                const buffer = stringToBuffer(serializedContents, "utf-8");
                return this.runtime.uploadBlob(buffer);
            },
            resolveHandle: async (handle: IFluidHandle<ArrayBufferLike>) => {
                const serializedContents = bufferToString(await handle.get(), "utf-8");
                const node: IBtreeInteriorNode<IFluidHandle<ArrayBufferLike>> | IBtreeLeafNode
                    = this.serializer.parse(serializedContents);
                return node;
            },
            compareHandles: ({ absolutePath: a }, { absolutePath: b }) => a < b ? -1 : a === b ? 0 : 1,
            discoverHandles,
        };
    }

    public getGCData(fullGC?: boolean | undefined): IGarbageCollectionData {
        // TODO: don't use blob manager, then this method becomes a noop
        const paths: string[] = this.btree.getAllHandles().map((handle) => handle.absolutePath);
        for (const handle of this.sequencedState.getValueHandles<IFluidHandle>(discoverHandles)) {
            paths.push(handle.absolutePath);
        }
        return { gcNodes: { "/": paths } };
    }

    /**
     * {@inheritDoc @fluidframework/shared-object-base#SharedObject.loadCore}
     * @internal
     */
    protected async loadCore(storage: IChannelStorageService) {
        const json = await readAndParse<IBtreeState<IFluidHandle<ArrayBufferLike>>>(storage, snapshotFileName);
        this.btree = await ChunkedBtree.load(
            json,
            this.createHandler(),
            (obj: unknown): obj is IFluidHandle<ArrayBufferLike> => (obj as IFluidHandle).IFluidHandle !== undefined,
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

        if (op.type === OpType.Flush) {
            if (local) {
                assert(this.pendingFlush === null, "Pending flush ack should have null state.");
                this.pendingFlush = undefined;
            }
            // A split-brain scenario could result in multiple concurrent flushes being sent,
            // and we should only process the latest one.
            if (op.refSequenceNumber > this.refSequenceNumberOfLastFlush) {
                this.refSequenceNumberOfLastFlush = op.refSequenceNumber;
                this.sequencedState.flush(op.refSequenceNumber);
                // TODO: this could retain downloaded chunks that are the same
                this.btree = this.btree.update(this.serializer.decode(op.update));
                this.sequencedState.evict(this.workingSetSize());
                this.emit(SharedPartialMapEvents.Flush, this.leaderTracker.isLeader());
                this.tryStartFlush();
            }
        } else {
            switch (op.type) {
                case OpType.Set:
                    this.sequencedState.set(op.key, this.serializer.decode(op.value), message.sequenceNumber);
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
                    this.btree = this.btree.clear();
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
                this.tryStartFlush();
            }
        }
    }
}

function *discoverHandles(value: any): Iterable<IFluidHandle> {
    if (!!value && typeof value === "object" && isFluidHandle(value)) {
        yield value;
    } else {
        yield *discoverHandlesI(value);
    }
}

function *discoverHandlesI(value: any): Iterable<IFluidHandle> {
    for (const [_, v] of Object.entries(value)) {
        if (!!v && typeof v === "object") {
            if (isFluidHandle(v)) {
                yield v;
            } else {
                yield* discoverHandlesI(v);
            }
        }
    }
}

function isFluidHandle(obj: unknown): obj is IFluidHandle {
    return (obj as IFluidHandle).IFluidHandle !== undefined;
}
