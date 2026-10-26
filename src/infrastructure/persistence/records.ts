import { BigIntType, EntitySchema } from '@mikro-orm/core';

export class WalletRecord {
  id!: string;
  playerId!: string;
  currency!: string;
  balanceCents!: bigint;
  version!: number;
  createdAt!: Date;
  updatedAt!: Date;
}

export const WalletRecordSchema = new EntitySchema<WalletRecord>({
  class: WalletRecord,
  tableName: 'wallets',
  schema: 'dwp',
  properties: {
    id: { type: 'uuid', primary: true },
    playerId: { type: 'string', fieldName: 'player_id' },
    currency: { type: 'string' },
    balanceCents: { type: new BigIntType('bigint'), fieldName: 'balance_cents' },
    version: { type: 'integer' },
    createdAt: { type: Date, fieldName: 'created_at' },
    updatedAt: { type: Date, fieldName: 'updated_at' },
  },
});
