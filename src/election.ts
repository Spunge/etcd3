
import { BigNumber } from 'bignumber.js';
import { Lease } from './lease';
import { EventEmitter } from 'events';

import { IKeyValue, IRangeResponse } from './rpc';
import { Namespace } from './namespace';
import { EtcdLeaseInvalidError, EtcdElectionNoLeaderError, EtcdElectionNotCampaigningError } from './errors';

export enum CampaignState {
  Idle = 'resigned',
  Leading = 'leading',
  Following = 'following',
}

/**
 * Etcd3 based election
 * For most part a port of the GO implementation
 * https://github.com/etcd-io/etcd/blob/master/clientv3/concurrency/election.go
 */
export class Election extends EventEmitter {
  private static readonly prefix: string = 'election';

  public lease: Lease;

  public campaignKey: string;
  public campaignRevision: BigNumber;
  public campaignState: CampaignState;

  private readonly keyPrefix: string;
  private readonly client: Namespace;
  private timeout: number;

  constructor(client: Namespace, prefix: string, timeout: number | undefined = 5) {
    super();

    this.keyPrefix = prefix;
    this.client = client;
    this.timeout = timeout;
    this.campaignState = CampaignState.Idle;
    this.createLease();

    // It's possible to loose a lease due to network, wonky etcd server, etc...
    this.lease.on('lost', async () => {
      await this.clearLease();
      this.createLease();
      this.setCampaignState(CampaignState.Idle);
    })
  }

  /**
   * Campaign in current election to be leader
   */
  public async campaign(value: string) {
    // Get the leaseID specifically to append it to key
    const leaseId = await this.lease.grant();
    const key = this.getKey(leaseId)

    // Create comparing query
    const res = await this.client.if(key, 'Create', '==', 0)
      .then(this.client.put(key).value(value).lease(leaseId))
      .else(this.client.get(key))
      .commit();

    this.campaignKey = key;
    this.campaignRevision = new BigNumber(res.header.revision);

    // Succeeded means that we got the 0 revision write
    if( ! res.succeeded) {
      // There has already been written to our key, which is weird
      const kv = res.responses[0].response_range.kvs[0];
      this.campaignRevision = new BigNumber(kv.create_revision);

      // Could be campaign was called with another value?
      try {
        return this.proclaim(value);
      } catch(err) {
        return this.resign()
          .then(() => { throw err });
      };
    }

    const prev_rev = this.campaignRevision.minus(1);

    // The oldest revision is leading
    const previous = await this.get_older_kvs(prev_rev);
    if(previous.kvs.length === 0) {
      this.setCampaignState(CampaignState.Leading);
    } else {
      // When there's older revisions, follow until we are the oldest
      this.setCampaignState(CampaignState.Following);

      // Wait for old key to be deleted, which would mean we are now leading
      const watcher = await this.client.watch()
        .prefix(this.getPrefix())
        .startRevision(prev_rev.toString())
        .create();

      // We don't want to be leader anymore after we resign
      // Resigned will be triggered when we stop campaigning: when we resign manually, or when our lease is lost
      this.on('resigned', () => watcher.cancel());

      // When previous key is deleted, we will lead the pack
      watcher
        .on('delete', async () => {
          await watcher.cancel();
          return this.setCampaignState(CampaignState.Leading);
        })
        .on('error', err => { throw err })
    }
  }

  /**
   * Change leaders value without starting new election
   */
  public proclaim(value: string) {
    if( ! this.campaignKey) {
      throw new EtcdElectionNotCampaigningError();
    }

    return this.client.if(this.campaignKey, 'Create', '==', this.campaignRevision.toString())
      .then(this.client.put(this.campaignKey).value(value).lease(this.lease.grant()))
      .commit()

      .then(async res => {
        if( ! res.succeeded) {
          await this.clearLease();
          this.setCampaignState(CampaignState.Idle);
          throw new EtcdElectionNotCampaigningError();
        }
      })
  }

  /**
   * Stop being leader
   */
  public async resign() {
    // Seems we're not leading anyway
    if( ! this.campaignKey) {
      return Promise.resolve();
    }

    await this.client.if(this.campaignKey, 'Create', '==', this.campaignRevision.toString())
      .then(this.client.delete().key(this.campaignKey))
      .commit();

    // Clear lease & Create new lease in case we want to campaign again
    await this.clearLease();
    this.createLease();
    this.setCampaignState(CampaignState.Idle);
  }

  /**
   * Get the current leader
   */
  public async leader(): Promise<IKeyValue> {
    const res = await this.client
      .getAll()
      .prefix(this.getPrefix())
      .sort("Create", "Ascend")
      .exec()

    if(res.kvs.length === 0) {
      throw new EtcdElectionNoLeaderError();
    }

    return Promise.resolve(res.kvs[0]);
  }

  public isLeading(): boolean {
    return this.campaignState === CampaignState.Leading;
  }

  public isFollowing(): boolean {
    return this.campaignState === CampaignState.Following;
  }

  public isIdle(): boolean {
    return this.campaignState === CampaignState.Idle;
  }

  public waitForCampaignState(state: CampaignState): Promise<any> {
    if(this.campaignState === state) {
      return Promise.resolve();
    } else {
      return new Promise(resolve => {
        this.on(state, resolve);
      });
    }
  }

  private createLease() {
    this.lease = this.client.lease(this.timeout);
  }

  private async clearLease() {
    // Unset campaignkey as that will be generated based on lease_id
    this.campaignKey = "";

    // Delete lease so we can create a new one
    try {
      await this.lease.revoke();
    } catch(err) {
      if( ! (err instanceof EtcdLeaseInvalidError)) {
        throw err
      }
    }
  }

  /**
   * Set campaignState which we use to keep track of our current role in the group
   */
  private setCampaignState(state: CampaignState, ...args: any[]) {
    this.campaignState = state;
    this.emit(state, ...args);
  }

  private get_older_kvs(maxCreateRevision: BigNumber): Promise<IRangeResponse> {
    return this.client.getAll()
      .prefix(this.getPrefix())
      .maxCreateRevision(maxCreateRevision.toString())
      .sort("Create", "Ascend")
      .exec();
  }

  /**
   * Get key of etcd object based on lease id
   */
  private getKey(leaseId: string): string {
    // Format as hexadecimal
    const leaseString = new BigNumber(leaseId).toString(16);
    return `${this.getPrefix()}${leaseString}`;
  }

  /**
   * Get prefix used for all election keys
   */
  private getPrefix(): string {
    return `${Election.prefix}/${this.keyPrefix}/`;
  }
}


