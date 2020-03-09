
import { expect } from 'chai';

import { Etcd3 } from '../src';
import { Election, CampaignState } from '../src/election';
import { Watcher } from '../src/watch';
import { EtcdError } from '../src/errors';
import { createTestClientAndKeys, tearDownTestClient } from './util';

async function getLeader(election: Election): Promise<string> {
  const kv = await election.leader();
  return kv.value.toString();
}

function getWatchers(client: Etcd3): Watcher[] {
  return (client as any).watchManager.watchers;
}

describe('election', () => {
  let client: Etcd3;
  let election0: Election;
  let election1: Election;
  let election2: Election;

  before(async () => {
    client = await createTestClientAndKeys();
  });
  after(async () => {
    await tearDownTestClient(client)
  });

  beforeEach(async () => {
    election0 = client.election('test', 1);
    election1 = client.election('test', 1);
    election2 = client.election('test', 1)
  });
  afterEach(async () => {
    await election0.resign();
    await election1.resign();
    await election2.resign();
  });

  describe('campaign()', () => {
    it('Elects first client campaigning as leader', async () => {
      await election0.campaign('0')
      await election1.campaign('1')

      expect(election0.isLeading()).to.be.true;
      expect(election1.isLeading()).to.be.false;
      expect(await getLeader(election0)).to.equal('0')
    });

    it('Does not start a watcher for old revisions when election is leading', async () => {
      await election0.campaign('0');

      expect(election0.isLeading()).to.be.true;
      expect(getWatchers(client)).to.have.length(0);
    });

    it('Starts watching old revisions when they exist', async () => {
      await election0.campaign('0');
      await election1.campaign('1');

      expect(await getLeader(election0)).to.equal('0')
      expect(election1.isLeading()).to.be.false;
      expect(getWatchers(client)).to.have.length(1);
    });

    it('Takes over leadership on leader resignation', async () => {
      await election0.campaign('0');
      await election1.campaign('1');
      await election0.resign();

      await election1.waitForCampaignState(CampaignState.Leading);

      expect(election0.isLeading()).to.be.false;
      expect(election1.isLeading()).to.be.true;
      expect(await getLeader(election0)).to.equal('1')
      expect(await getLeader(election1)).to.equal('1')
    });

    it('Takes over leadership when leader fails', async () => {
      await election0.campaign('0');
      await election1.campaign('1');
      // Cast to any to access private methods (what typescript?) & kill lease of 0
      (election0.lease as any).emitLoss(new EtcdError('forced fail'));
      await election1.waitForCampaignState(CampaignState.Leading);

      expect(election0.isLeading()).to.be.false;
      expect(election1.isLeading()).to.be.true;
      expect(await getLeader(election1)).to.equal('1')
    });

    it('Only takes over leadership on election with oldest lease', async () => {
      await election0.campaign('0');
      await election1.campaign('1');
      await election2.campaign('2');

      expect(election0.isLeading()).to.be.true;
      expect(election1.isLeading()).to.be.false;
      expect(election2.isLeading()).to.be.false;

      (election0.lease as any).emitLoss(new EtcdError('forced fail'));
      await election1.waitForCampaignState(CampaignState.Leading);

      expect(election0.isLeading()).to.be.false;
      expect(election1.isLeading()).to.be.true;
      expect(election2.isLeading()).to.be.false;

      (election1.lease as any).emitLoss(new EtcdError('forced fail'));
      await election2.waitForCampaignState(CampaignState.Leading);

      expect(election0.isLeading()).to.be.false;
      expect(election1.isLeading()).to.be.false;
      expect(election2.isLeading()).to.be.true;
    })

    describe('Multiple subsequent calls', () => {
      it('Proclaims when with the same lease', async () => {
        await election0.campaign('0')
        await election0.campaign('2')

        expect(await getLeader(election0)).to.equal('2')
        expect(election0.isLeading()).to.be.true;
        expect(getWatchers(client)).to.have.length(0);
      });

      it('Does not start multiple watchers when election is following', async () => {
        await election0.campaign('0')
        await election1.campaign('1')
        await election1.campaign('2')

        expect(election0.isLeading()).to.be.true;
        expect(election1.isLeading()).to.be.false;
        expect(getWatchers(client)).to.have.length(1);
      });
    });

    describe('Recampaigning on resign', () => {
      it('Creates new lease', async () => {
        await election0.campaign('0');
        await election1.campaign('1');

        const wait_for_recampaign = new Promise(resolve => {
          election0.once('resigned', async () => {
            await election0.campaign('0');
            resolve();
          });
        });

        expect(election0.isLeading()).to.be.true;
        expect(election1.isLeading()).to.be.false;

        await Promise.all([wait_for_recampaign, election0.resign()]);

        // As election0 is now re-campaigning, election1 should be leading after watcher triggers
        await election1.waitForCampaignState(CampaignState.Leading);

        expect(election0.isIdle()).to.be.false;
        expect(election1.isLeading()).to.be.true;
      })
    })
  })

  describe('proclaim()', () => {
    it('Can use proclaim to change value without re-electing', async () => {
      await election0.campaign('0')
      await election1.campaign('1')

      expect(await getLeader(election0)).to.equal('0')

      await election0.proclaim('2');
      expect(await getLeader(election0)).to.equal('2')

      expect(election0.isLeading()).to.be.true;
      expect(election1.isLeading()).to.be.false;
    });
  })

  describe('leader()', () => {
    it('Reports campaigned value of leader by quering etcd', async () => {
      await election0.campaign('0');
      await election1.campaign('1');

      expect(await getLeader(election1)).to.equal('0');
    })
  })
})
