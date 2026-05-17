// Client-version metadata for the InitConnection versioning spec.
// Merged into the InitConnection payload by BaseClient on every
// (re)connect so the gateway audit log can attribute connections to a
// specific SDK build without consumers having to wire anything.

import packageJson from "../package.json" with { type: "json" };

const CLIENT_SDK_NAME = "ts";

/**
 * Cached version metadata. Computed lazily so module load does not pay
 * a runtime detection cost when the SDK is never connected, but
 * memoised after the first call.
 */
let cachedMeta:
  | {
      clientVersion: string;
      clientSdk: string;
      clientBuildInfo: { runtime: string; os: string };
    }
  | undefined;

/**
 * Returns this SDK's version + build-info, shaped to merge directly
 * onto an InitConnection message. Browser builds default runtime/os to
 * "browser"/the user-agent string because process.* is not available;
 * Node.js builds use process.versions.node and process.platform/arch.
 */
export function clientVersionMeta(): {
  clientVersion: string;
  clientSdk: string;
  clientBuildInfo: { runtime: string; os: string };
} {
  if (cachedMeta !== undefined) {
    return cachedMeta;
  }

  const version = (packageJson as { version?: string }).version ?? "unknown";

  let runtime = "unknown";
  let os = "unknown";
  if (typeof process !== "undefined" && process.versions && process.versions.node) {
    runtime = `node${process.versions.node}`;
    os = `${process.platform}/${process.arch}`;
  } else if (typeof navigator !== "undefined") {
    runtime = "browser";
    os = navigator.userAgent ?? "browser";
  }

  cachedMeta = {
    clientVersion: version,
    clientSdk: CLIENT_SDK_NAME,
    clientBuildInfo: { runtime, os },
  };
  return cachedMeta;
}
