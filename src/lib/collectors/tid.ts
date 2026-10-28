/**
 * atproto TID → creation time. A TID is 13 chars of base32-sortable encoding a 64-bit int:
 * top bit 0, then 53 bits of microseconds since epoch, then a 10-bit clock id.
 */
const S32 = "234567abcdefghijklmnopqrstuvwxyz";
const TID_RE = /^[234567abcdefghij][234567abcdefghijklmnopqrstuvwxyz]{12}$/;

/** Creation time encoded in a TID rkey, or null if the rkey isn't a TID. */
export function tidToDate(rkey: string): Date | null {
  if (!TID_RE.test(rkey)) return null;
  let n = 0n;
  for (const ch of rkey) n = n * 32n + BigInt(S32.indexOf(ch));
  return new Date(Number(n >> 10n) / 1000);
}
