import { IEvent, ITelemetryLogger } from '@fluidframework/common-definitions';
import { TypedEventEmitter } from '@fluidframework/common-utils';
import { AttachState } from '@fluidframework/container-definitions';
import { IFluidDataStoreRuntime } from '@fluidframework/datastore-definitions';
import { assert } from './Common';

/**
 * Events which may be emitted by `LeaderTracker`.
 */
 export interface ILeaderEvents extends IEvent {
	(event: 'promoted', listener: () => void);
	(event: 'demoted', listener: () => void);
}

/**
 * Tracks who is the leader in the quorum of connected clients.
 */
 export class LeaderTracker extends TypedEventEmitter<ILeaderEvents> {
	/** Indicates if the client is the oldest member of the quorum. */
	private currentIsOldest: boolean;

    public constructor(private readonly runtime: IFluidDataStoreRuntime, private readonly logger: ITelemetryLogger) {
        super();
        // This code is somewhat duplicated from OldestClientObserver because it currently depends on the container runtime
		// which SharedTree does not have access to.
		// TODO:#55900: Get rid of copy-pasted OldestClientObserver code
		const quorum = this.runtime.getQuorum();
		this.currentIsOldest = this.computeIsOldest();
		quorum.on('addMember', this.updateOldest);
		quorum.on('removeMember', this.updateOldest);
        this.runtime.on('connected', this.updateOldest);
		this.runtime.on('disconnected', this.updateOldest);
    }

    public isLeader(): boolean {
        return this.currentIsOldest;
    }

    /**
	 * Computes the oldest client in the quorum, true by default if the container is detached and false by default if the client isn't connected.
	 * TODO:#55900: Get rid of copy-pasted OldestClientObserver code
	 */
	private computeIsOldest(): boolean {
		// If the container is detached, we are the only ones that know about it and are the oldest by default.
		if (this.runtime.attachState === AttachState.Detached) {
			return true;
		}

		// If we're not connected we can't be the oldest connected client.
		if (!this.runtime.connected) {
			return false;
		}

		assert(this.runtime.clientId !== undefined, 'Client id should be set if connected.');

		const quorum = this.runtime.getQuorum();
		const selfSequencedClient = quorum.getMember(this.runtime.clientId);
		// When in readonly mode our clientId will not be present in the quorum.
		if (selfSequencedClient === undefined) {
			return false;
		}

		const members = quorum.getMembers();
		for (const sequencedClient of members.values()) {
			if (sequencedClient.sequenceNumber < selfSequencedClient.sequenceNumber) {
				return false;
			}
		}

		// No member of the quorum was older
		return true;
	}

    	/**
	 * Re-computes currentIsOldest and emits an event if it has changed.
	 * TODO:#55900: Get rid of copy-pasted OldestClientObserver code
	 */
	private readonly updateOldest = () => {
		const oldest = this.computeIsOldest();
		if (this.currentIsOldest !== oldest) {
			this.currentIsOldest = oldest;
			if (oldest) {
				this.emit('promoted');
				this.logger.sendTelemetryEvent({ eventName: 'BecameOldestClient' });
			} else {
				this.emit('demoted');
			}
		}
	};
}
