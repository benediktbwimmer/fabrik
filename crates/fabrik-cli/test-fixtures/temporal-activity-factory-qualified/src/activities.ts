export interface DB {
  get(key: string): Promise<string>;
}

export const createActivities = (db: DB) => ({
  async greet(prefix: string): Promise<string> {
    const name = await db.get("name");
    return `${prefix}:${name}`;
  },
});
