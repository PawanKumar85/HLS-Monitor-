const REDACTED = "[redacted]";
const SECRET_FIELD = /token|secret|password|authorization|credential|cookie|api.?key/i;

/** Keep the URL while removing credentials, query parameters and fragments. */
export function redactUrl(value: string): string {
  try {
    const url = new URL(value);
    url.username = "";
    url.password = "";
    url.search = "";
    url.hash = "";

    return url.toString();
  } catch {
    return "[invalid URL redacted]";
  }
}

export function redactError(value: string): string {
  return value
    .replace(/https?:\/\/[^\s"'<>]+/gi, url => redactUrl(url))
    .replace(/(token|secret|password|authorization|api[_-]?key)\s*[:=]\s*[^\s,;]+/gi, `$1=${REDACTED}`)
    .replace(/Bearer\s+[^\s,;]+/gi, `Bearer ${REDACTED}`)
    .slice(0, 2000);
}

export function redactFields(fields: string[]): string[] {
  const result = [...fields];

  // Redis stream fields alternate between a key and its value.
  for (let index = 1; index < fields.length; index += 2) {
    const key = fields[index - 1]!;
    const value = fields[index]!;

    if (SECRET_FIELD.test(key)) result[index] = REDACTED;
    else if (/url/i.test(key)) result[index] = redactUrl(value);
    else result[index] = redactError(value);
  }

  return result;
}
