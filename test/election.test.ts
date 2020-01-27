
import { expect } from 'chai';

import { Etcd3 } from '../src';
import { Election } from '../src/election';
import { createTestClientAndKeys, tearDownTestClient } from './util';

function get_leader(election: Election): Promise<string> {
  return election.leader().then(kv => kv.value.toString());
}

describe('Election', () => {
  let client: Etcd3;
  let election0: Election;
  let election1: Election;

  before(async () => {
    client = await createTestClientAndKeys();
  });
  after(async () => {
    await tearDownTestClient(client)
  });

  beforeEach(async () => {
    election0 = client.election('test', 1);
    election1 = client.election('test', 1);
  });
  afterEach(async () => {
    await election0.resign();
    await election1.resign();
  });

  it('Elects first client campaigning as leader', async () => {
    await election0.campaign('0')
    await election1.campaign('1')

    await expect(get_leader(election0)).to.eventually.equal('0')
    await expect(get_leader(election1)).to.eventually.equal('0')
  });

  it('Gracefully handles multiple calls to campaign()', async () => {
    await election0.campaign('0')
    await election0.campaign('2')

    await expect(get_leader(election0)).to.eventually.equal('2')
  });

  it('Can use proclaim to change value without re-electing', async () => {
    await election0.campaign('0')
    await election1.campaign('1')

    await expect(get_leader(election0)).to.eventually.equal('0')

    await election0.proclaim('2');
    await expect(get_leader(election0)).to.eventually.equal('2')
  });

  it('Takes over leadership on leader resignation', async () => {
    await election0.campaign('0');
    await election1.campaign('1');
    await election0.resign();

    await expect(get_leader(election0)).to.eventually.equal('1')
    await expect(get_leader(election1)).to.eventually.equal('1')
  });

  it('Takes over leadership when leader fails', async () => {
    await election0.campaign('0');
    await election1.campaign('1');
    // Kill lease of 0
    await election0.kill();

    await expect(get_leader(election1)).to.eventually.equal('1')
  });

  it('Reports leader state by quering etcd', async () => {
    await election0.campaign('0');
    await election1.campaign('1');

    const isLeader0 = await election0.isLeader();
    const isLeader1 = await election1.isLeader();

    await expect(isLeader0).to.equal(true);
    await expect(isLeader1).to.equal(false);
  })
})
