import { Migration } from '@mikro-orm/migrations';

export class Migration20261025000100 extends Migration {
  override up(): void {
    this.addSql('create schema dwp;');
  }

  override down(): void {
    this.addSql('drop schema dwp;');
  }
}
