import type Redis from "ioredis";
import type { Env } from "../config";

export type QuotaState = {
  activeUploads: number;
  dailyUploads: number;
};

function secondsUntilEndOfDay(now: Date) {
  const end = new Date(now);
  end.setHours(23, 59, 59, 999);
  return Math.max(1, Math.ceil((end.getTime() - now.getTime()) / 1000));
}

export class UploadQuotaService {
  constructor(
    private readonly redis: Redis,
    private readonly config: Env
  ) {}

  getLimits() {
    return {
      concurrentLimit: this.config.UPLOAD_CONCURRENT_LIMIT,
      dailyLimit: this.config.UPLOAD_DAILY_LIMIT,
    };
  }

  private activeKey(adminId: string) {
    return `upload:quota:${adminId}:active`;
  }

  private dailyKey(adminId: string, now: Date) {
    const datePart = now.toISOString().slice(0, 10);
    return `upload:quota:${adminId}:${datePart}:count`;
  }

  async getCurrentQuota(adminId: string, now: Date): Promise<QuotaState> {
    const execResults = await this.redis
      .multi()
      .get(this.activeKey(adminId))
      .get(this.dailyKey(adminId, now))
      .exec();

    const activeRaw = execResults?.[0]?.[1] as string | null | undefined;
    const dailyRaw = execResults?.[1]?.[1] as string | null | undefined;

    return {
      activeUploads: Number(activeRaw ?? 0),
      dailyUploads: Number(dailyRaw ?? 0),
    };
  }

  async claim(adminId: string, now: Date): Promise<QuotaState> {
    const current = await this.getCurrentQuota(adminId, now);

    if (current.activeUploads >= this.config.UPLOAD_CONCURRENT_LIMIT) {
      throw new Error("concurrent_quota_exceeded");
    }

    if (current.dailyUploads >= this.config.UPLOAD_DAILY_LIMIT) {
      throw new Error("daily_quota_exceeded");
    }

    const ttl = secondsUntilEndOfDay(now);

    const results = await this.redis
      .multi()
      .incr(this.activeKey(adminId))
      .expire(this.activeKey(adminId), ttl)
      .incr(this.dailyKey(adminId, now))
      .expire(this.dailyKey(adminId, now), ttl)
      .exec();

    const [activeAfter, dailyAfter] = [results?.[0]?.[1], results?.[2]?.[1]];

    return {
      activeUploads: Number(activeAfter ?? current.activeUploads + 1),
      dailyUploads: Number(dailyAfter ?? current.dailyUploads + 1),
    };
  }

  async release(adminId: string) {
    const activeKey = this.activeKey(adminId);
    await this.redis.eval(
      `local current = redis.call('get', KEYS[1])
       if not current then return 0 end
       local next = tonumber(current) - 1
       if next <= 0 then
         redis.call('del', KEYS[1])
         return 0
       end
       redis.call('set', KEYS[1], next)
       return next`,
      1,
      activeKey
    );
  }
}
