import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mongoUri } from '../src/config/mongo.config.js';

test('MongoDB: no default credentials; MONGO_URI wins; user and password are URL-encoded', () => {
  assert.throws(() => mongoUri({}), /Set MONGO_URI/);
  assert.throws(() => mongoUri({ MONGO_USER: 'admin' }), /MONGO_PASSWORD/);
  assert.equal(mongoUri({ MONGO_URI: 'mongodb://u:p@db:27017/x' }), 'mongodb://u:p@db:27017/x');
  assert.equal(mongoUri({ MONGO_USER: 'ops', MONGO_PASSWORD: 'p@ss/w:rd' }, 'gate'), 'mongodb://ops:p%40ss%2Fw%3Ard@127.0.0.1:27017/gate?authSource=admin');
});
