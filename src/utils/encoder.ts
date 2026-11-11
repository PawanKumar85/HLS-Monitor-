/**
 * Serializes any JavaScript object into a flat array of string key-value pairs
 * compatible with Redis Stream `XADD` (`[field1, val1, field2, val2, ...]`).
 *
 * Encodings applied:
 * - strings: stored as-is
 * - numbers: integer as toString(), floats (e.g. latency) as toFixed(2)
 * - booleans: "true" | "false"
 * - Date: ISO string
 * - objects / arrays: JSON.stringify()
 * - undefined / null: omitted automatically
 */
export function encodeRedisStreamFields<T extends Record<string, unknown>>(data: T): string[] {
  const fields: string[] = [];

  for (const [key, value] of Object.entries(data)) {
    if (value === undefined || value === null) {
      continue;
    }

    fields.push(key);

    if (typeof value === "string") {
      fields.push(value);
    } else if (typeof value === "number") {
      fields.push(Number.isInteger(value) ? value.toString() : value.toFixed(2));
    } else if (typeof value === "boolean") {
      fields.push(value ? "true" : "false");
    } else if (value instanceof Date) {
      fields.push(value.toISOString());
    } else if (typeof value === "object") {
      fields.push(JSON.stringify(value));
    } else {
      fields.push(String(value));
    }
  }

  return fields;
}

/**
 * Convenience alias
 */
export const encodeStreamFields = encodeRedisStreamFields;

/**
 * Deserializes raw Redis Stream array [k1, v1, k2, v2...] into a JavaScript object.
 * Parses JSON, booleans, and numbers automatically.
 */
export function decodeRedisStreamFields<T = Record<string, unknown>>(rawFields: string[]): T {
  const result: Record<string, unknown> = {};

  for (let i = 0; i < rawFields.length; i += 2) {
    const key = rawFields[i];
    const value = rawFields[i + 1];

    if (!key || value === undefined) continue;

    // Decode booleans
    if (value === "true") {
      result[key] = true;
    } else if (value === "false") {
      result[key] = false;
    }
    // Decode JSON objects/arrays
    else if (
      (value.startsWith("{") && value.endsWith("}")) ||
      (value.startsWith("[") && value.endsWith("]"))
    ) {
      try {
        result[key] = JSON.parse(value);
      } catch {
        result[key] = value;
      }
    }
    // Decode numeric strings (e.g. status code, latency)
    else if (!isNaN(Number(value)) && value.trim() !== "") {
      result[key] = Number(value);
    }
    // Plain string
    else {
      result[key] = value;
    }
  }

  return result as T;
}

export const decodeStreamFields = decodeRedisStreamFields;

