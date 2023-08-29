/*!
 * Copyright (c) Microsoft Corporation and contributors. All rights reserved.
 * Licensed under the MIT License.
 */

import { Delta, RemoveAgnosticVisitor, DeltaVisitor, FieldKey, visitDelta } from "../core";
import { IdAllocator, TreeIndex } from "../feature-libraries";
import { brand } from "../util";

export function removeAgnosticVisitFromRemoveAwareVisit(
	treeIndex: TreeIndex,
	rootIdAllocator: IdAllocator,
	changeIdAllocator: IdAllocator,
): (d: Delta.Root, visitor: RemoveAgnosticVisitor) => void {
	return (d: Delta.Root, visitor: RemoveAgnosticVisitor) => {
		const rootVisitor = visitor.fork();
		const removedContentVisitor = visitor.fork();
		let modificationVisitor = visitor;
		const wrappedVisitor: DeltaVisitor = {
			enterNode: (index: number): void => {
				modificationVisitor.enterNode(index);
			},
			exitNode: (index: number): void => {
				modificationVisitor.exitNode(index);
			},
			enterField: (key: FieldKey): void => {
				modificationVisitor.enterField(key);
			},
			exitField: (key: FieldKey): void => {
				modificationVisitor.exitField(key);
			},
			onDelete: (index: number, count: number) => {
				modificationVisitor.onDelete(index, count);
			},
			onInsert: (index: number, content: Delta.ProtoNodes) => {
				modificationVisitor.onInsert(index, content);
			},
			onMoveOut: (index: number, count: number, id: Delta.MoveId) => {
				modificationVisitor.onMoveOut(index, count, id);
			},
			onMoveIn: (index: number, count: number, id: Delta.MoveId) => {
				modificationVisitor.onMoveIn(index, count, id);
			},
			onRemove: (index: number, count: number, nodeId: Delta.RemovedNodeId) => {
				// TODO: update the RemoveFreeDeltaVisitor contract to support batch-detaching contiguous nodes
				// into individual roots.
				let moveId: Delta.MoveId = brand(changeIdAllocator(count));
				for (let iNode = 0; iNode < count; iNode += 1) {
					const fieldKey = treeIndex.createEntry(
						{ ...nodeId, minor: nodeId.minor + iNode },
						brand(rootIdAllocator()),
					);
					// We need to create new IDs to represent the combination of the RemovedNodeId
					// major and minor.
					visitor.onMoveOut(index, 1, moveId);
					rootVisitor.enterField(fieldKey);
					rootVisitor.onMoveIn(0, 1, moveId);
					rootVisitor.exitField(fieldKey);
					moveId = brand((moveId as unknown as number) + 1);
				}
			},
			enterRemovedNode: (nodeId: Delta.RemovedNodeId) => {
				const fieldKey = treeIndex.getFieldKey(nodeId);
				removedContentVisitor.enterField(fieldKey);
				removedContentVisitor.enterNode(0);
				modificationVisitor = removedContentVisitor;
			},
			exitRemovedNode: (nodeId: Delta.RemovedNodeId) => {
				const fieldKey = treeIndex.getFieldKey(nodeId);
				removedContentVisitor.exitNode(0);
				removedContentVisitor.exitField(fieldKey);
				modificationVisitor = visitor;
			},
			onRestore: (index: number, count: number, nodeId: Delta.RemovedNodeId) => {
				let moveId: Delta.MoveId = brand(changeIdAllocator(count));
				for (let iNode = 0; iNode < count; iNode += 1) {
					const fieldKey = treeIndex.getFieldKey({
						...nodeId,
						minor: nodeId.minor + iNode,
					});
					rootVisitor.enterField(fieldKey);
					rootVisitor.onMoveOut(0, 1, moveId);
					rootVisitor.exitField(fieldKey);
					visitor.onMoveIn(index, 1, moveId);
					moveId = brand((moveId as unknown as number) + 1);
				}
			},
			onMoveOutRemovedNodes: (
				nodeId: Delta.RemovedNodeId,
				count: number,
				id: Delta.MoveId,
			) => {
				let moveId: Delta.MoveId = id;
				for (let iNode = 0; iNode < count; iNode += 1) {
					const fieldKey = treeIndex.getFieldKey({
						...nodeId,
						minor: nodeId.minor + iNode,
					});
					rootVisitor.enterField(fieldKey);
					rootVisitor.onMoveOut(0, 1, moveId);
					rootVisitor.exitField(fieldKey);
					moveId = brand((moveId as unknown as number) + 1);
				}
			},
		};
		visitDelta(d, wrappedVisitor);
		rootVisitor.free();
		removedContentVisitor.free();
	};
}
