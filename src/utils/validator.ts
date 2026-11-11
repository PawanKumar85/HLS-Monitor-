/**
 * Validates that the stream URL uses http/https protocol and ends with .m3u8
 */
export function validateHlsUrl(streamUrl: string): URL {
    let parsed: URL;
    try {
        parsed = new URL(streamUrl);
    } catch {
        throw new Error(`Invalid URL format: "${streamUrl}". Must be a valid absolute URL.`);
    }

    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
        throw new Error(`Invalid protocol "${parsed.protocol}". Stream URL must use http: or https:.`);
    }

    if (!parsed.pathname.toLowerCase().endsWith(".m3u8")) {
        throw new Error(`Invalid file extension: "${parsed.pathname}". Stream URL must end with .m3u8.`);
    }
    return parsed;
}

