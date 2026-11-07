# Hermes Chrome tab watchdog

A directory plugin for Hermes Agent. The observer and transparent CDP forwarder run in a background thread **inside Hermes**, not in a separately launched process. Hermes unload/reload stops the thread, listeners and ledger owner. A native script-only cron requests cleanup every 15 minutes. Neither observation nor cleanup calls a model.

## Requirements and boundaries

- Linux, Python 3.11+, Hermes plugin SDK with `tool_execution` middleware and `ctx.on_unload`. Loader/unload compatibility is exercised on unmodified official v0.22.5 and the development installation.
- `websockets>=15,<16` available to the Python interpreter running Hermes. This plugin never installs packages or launches Chrome.
- An existing loopback HTTP Chrome CDP endpoint. No cookie/profile copying, browser restart or custom Hermes core patches.
- Configure native browser traffic through the plugin's loopback forwarding port. The forwarder is internal plugin machinery, retained to attribute actual target-specific CDP commands rather than guessing from tool code.
- A running Hermes process with this plugin loaded owns observation. If Hermes is stopped, cleanup fails closed. The transient cron client never starts a replacement observer.

## Install

Install from GitHub through Hermes, review the dependency prompt, and leave the plugin disabled until routing is configured:

```sh
hermes plugins install web3blind/hermes-chrome-tab-watchdog --no-enable
```

The native installer treats this as an unreviewed community source. Its security scanner may emit **CAUTION** for the loopback HTTP/CDP connection, the Linux fd-relative Unix-socket helper, and disposable Chromium test launchers. A non-interactive install can therefore be blocked before installation. Review the reported findings and source before explicitly approving them through Hermes; do not disable scanning. The command above intentionally does not bypass this gate. The runtime does not launch Chromium; the launchers are isolated test fixtures.

Perform initial setup while no browser tasks are running. Apply the settings below, then enable the plugin with `hermes plugins enable chrome-tab-watchdog`. Check `--health` before resuming browser work. Routing through a plugin that is not loaded makes browser tools unavailable. This repository is not part of the curated Hermes plugin catalog.

## Settings

All settings live in `config.yaml` under `plugins.entries.chrome-tab-watchdog.settings`. The native Plugins settings surface uses `plugin.yaml`'s `config_schema`. No independent policy JSON or `.env` settings file.

Example for an existing Chrome at port 9222:

```yaml
plugins:
  entries:
    chrome-tab-watchdog:
      settings:
        idle_hours: 6
        duplicate_hours: 2
        blank_minutes: 30
        upstream: http://127.0.0.1:9222
        proxy_host: 127.0.0.1
        proxy_port: 9223
browser:
  cdp_url: http://127.0.0.1:9223
  tab_cleanup_enabled: false
```

Use Hermes configuration commands rather than overwriting config.yaml:

```sh
hermes config set plugins.entries.chrome-tab-watchdog.settings.idle_hours 6
hermes config set plugins.entries.chrome-tab-watchdog.settings.duplicate_hours 2
hermes config set plugins.entries.chrome-tab-watchdog.settings.blank_minutes 30
hermes config set plugins.entries.chrome-tab-watchdog.settings.upstream http://127.0.0.1:9222
hermes config set plugins.entries.chrome-tab-watchdog.settings.proxy_port 9223
hermes config set browser.cdp_url http://127.0.0.1:9223
hermes config set browser.tab_cleanup_enabled false
```

Install the directory through the native Hermes plugin installer and enable `chrome-tab-watchdog`. No service unit, daemon command or host-specific launcher is required. Do not route an active browser to the forwarding port before the plugin is loaded. Choose an unused loopback port and never expose CDP publicly. Existing independent browser clients bypassing this route are outside the exact-tool attribution contract.

Durations must be positive finite numbers. Unknown fields and invalid values disable cleanup while preserving transport where its routing settings remain valid. Effective values and diagnostics appear in `--health`. Settings reload with the plugin. Changing routing/settings while loaded fences cleanup until a clean reload; environment/live/profile route bypasses also fence cleanup before dispatching the browser call once. Multiple Hermes processes elect one profile owner before opening SQLite; contenders attach without taking over its socket or resetting coverage. Incompatible contenders fence the owner instead of silently changing policy.

## Native cron and diagnostics

Point a native Hermes cron at the installed directory's absolute `cron.py` path, with `no_agent=true`, schedule `*/15 * * * *`, normal delivery local, and an explicit appropriate failure-notification destination. Configure this once through Hermes cron management. Retain an existing job rather than adding a duplicate. `cron.py` is a normal short-lived scheduler client, not a daemon.

From the installed plugin directory, with Hermes' Python environment:

```sh
python -m tabwatch_plugin.cli --health
python -m tabwatch_plugin.cli --dry-run
python cron.py
```

`cron.py` applies the policy. Dry-run never closes targets. Healthy ticks without closures are silent; observation/control failures return nonzero. Health includes the hosting Hermes PID, a service-incarnation token, effective policy and sanitized error metadata. No URLs, titles, DOM text, code, credentials or typed input are persisted in the ledger. State is profile-scoped under `get_hermes_home()/state/chrome-tab-watchdog`; full URL comparisons use a private keyed HMAC.

## Cleanup block diagnostics

A failure of an attached process's read-only health probe does not imply lost browser activity. The probe is retried without poisoning the owner. A disconnected reader of a parsed health response is also harmless. Invalid settings, missed browser leases, routing bypasses, telemetry errors and uncertain mutating control requests still durably disable cleanup.

Gap records contain an enumerated cause, timestamp and PID, plus sanitized failed-sweep evidence when available. Health reports cleanup_gap_reason; the CLI distinguishes a latched cleanup block from a dead observer. Legacy gap records do not contain enough data to identify the original trigger. A reload accepts the previous gap baseline but starts fresh observation grace; it must never reuse the unobserved interval to close tabs early.

Failed sweeps expose `last_cleanup_error` in health and a matching `diagnostic` in
the sweep error. Its bounded fields are a fixed stage, allowlisted exception
category (`kind`), allowlisted CDP `method` (or null), timestamp (`at`), fixed
`reason`, and close `progress`: `not_attempted`, `outcome_unknown`,
`acknowledged`, or `disappearance_verified`. `close_refused` identifies a negative
close acknowledgement; `close_unverified` means the acknowledged target remained
in the verification inventory. Other failures use `close_failed`, with stage and
progress distinguishing discovery/inventory failures, uncertain close outcomes,
verification errors and post-close ledger failures. Exception text, arbitrary
exception class/method names, URLs, titles and command arguments are never stored
in this evidence.

The latest incident is persisted alongside the gap record and retained across
reloads and later fences; it is historical evidence, not a claim that the current
observer is failing. Legacy records have no incident detail. Persistence failure
still leaves the in-memory diagnostic and cleanup latch intact. A failed sweep
keeps the permanent `cleanup_failure` fence for that owner: no automatic recovery
or retry of an uncertain close. Existing reload grace/old-lease holds are unchanged.
The control client preserves validated sweep errors and any verified `closed`
receipts, including a disappearance verified before a ledger write failed. The CLI
prints that receipt and exits nonzero; an acknowledged but unverified close is
never included in `closed`. Lease acknowledgements and other control rejections
remain errors.

## Idle timer continuity

A disconnected read-only dry-run client does not disable cleanup. A lost reply after a completed mutation preserves the registered lease or completed
sweep; failures during mutation still fence cleanup. A client without a lease
acknowledgement continues to fence before dispatching its browser action. Transient control, lease and activity-telemetry failures can recover
automatically after 1900 seconds (the maximum browser-call lease), once fresh complete
observation is available and no leases or pending commands remain. Every new transient
fence restarts the full wait; routing, settings and ledger failures remain blocked for
repair. Recovery accepts only the exact fence it waited for and starts new observation
grace, including a fresh blank-page grace; old unobserved time never earns closure.
Health exposes `control_recovery_wait_seconds`. This is a minimum wait, not a promise
that a broken browser or unsafe routing will recover without intervention.
A reload with an existing gap also holds cleanup for the maximum old-owner lease
duration while observation continues; health reports `startup_cleanup_wait_seconds`.
External cron workers attach to the observer but never acquire ownership, so a
long-running job cannot capture the browser route with stale code during a gateway
reload. Health reports the actual owner's PID and `runtime_version`.

Client CDP transport loss alone is not loss of the independent observer. A completed or interrupted command attributed to a known page updates only that page's tool time; unrelated pages retain their inactivity history. Unattributed activity, observer loss, timing gaps and browser-epoch changes still restart the required global safety grace. Health exposes a bounded recent_gaps list containing only fixed cause labels and allowlisted protocol-method names, never command arguments.

Six-hour close acceptance includes real closure of an isolated disposable Chromium page with a synthetic test clock, exact-boundary checks and an active-lease veto. Live page timestamps are never aged or rewritten to force acceptance.

## Policy and safety

- Exact `about:blank`: 30 minutes since first observation, not claimed historical creation time. Query/fragment variants are not exact blank.
- Retain the last exact `about:blank`, even when other pages exist. Never close the last admitted page. These guards are rechecked against fresh inventory before each actual close, including within a batch.
- Exact full URL duplicate losers: two hours without use, retaining the most recently used tab. Query and fragment are significant; winner changes or use reset the loser interval.
- Other admitted pages, including editors: six hours without observed use. No permanent site exceptions. An idle editor may contain unsaved text; it is still subject to expiry.
- Native trusted input/focus and actual target-specific browser commands count as use. Periodic inventory and the observer's own setup commands do not.
- Unknown attribution, missing coverage, observation gaps, in-flight requests and bounded leases fence cleanup. A reload begins fresh observation grace; it does not infer inactivity during downtime.
- Supported credential/payment browser tools also hold a bounded cleanup lease when those optional tools are installed. The plugin never reads or stores their protected values.
- Revalidate browser identity, URL, activity, winner and health immediately before closing. Verify target disappearance; there is no fixed tab-count cap.
- Each applied sweep has a fixed 60-second monotonic admission budget, checked before starting another target and again immediately before sending a close after discovery/inventory and revalidation. Budget exhaustion is a successful partial sweep, not a cleanup fault. A close already sent is still verified to completion; only verified disappearances enter `closed`. New global browser work/fences, unhealthy observation or a cleanup error stop further targets.
- The CLI allows 180 seconds for a sweep response, including bounded verification of a close admitted before the deadline. The budget does not cancel an in-flight close or weaken the permanent uncertain-close fence. Control receipts retain schema validation and the 1 MiB response-frame bound; candidate/closed lists are not capped at five.
- Ledger/observer/lease failures disable cleanup, not browser command dispatch. Raw request/receipt forwarding is preserved and browser actions are never retried by this plugin.

The internal forwarding listener remains part of the browser route. A complete plugin unload, missing dependency, invalid transport configuration or port-bind failure can make that route unavailable. Restore the original direct browser endpoint **before disabling/uninstalling the plugin**. This does not claim that every possible plugin/runtime failure is transparent to browser transport.

## Catalog review disclosures

Hermes >=0.22.5 is required. The only registered capability is `tool_execution` middleware; there are no model tools, hooks, credential backends or required secrets.

The plugin attaches CDP sessions and installs page activity listeners to observe trusted input/focus without recording typed content. It forwards browser protocol traffic through a loopback listener and uses a profile-local Unix control socket and background observer thread inside Hermes. Runtime state (SQLite, a generated local URL-HMAC key, locks and sanitized gap markers) belongs to the profile's `state/chrome-tab-watchdog` directory. The HMAC key is not a provider credential. No telemetry, external API calls, self-updater or runtime shell/subprocess launcher is included. Disposable test fixtures launch their own unauthenticated Chromium only.

The admission scanner reports `proc_access` for `/proc/self/fd` socket addressing on Linux; the fd-relative helper handles long Unix-socket paths. The route check conditionally imports the read-only `tools.browser_tool_cdp` (released Hermes) and `tools.browser_session_cdp` (current development Hermes) resolvers to detect effective live/secret-scoped CDP routing overrides. These are internal APIs and can change; disclose this dependency even when a scanner does not resolve conditional imports. Missing both modules, import dependency failures, incomplete APIs and resolver exceptions all fence cleanup instead of bypassing route validation. The plugin never replaces or mutates core functions. Native plugins execute in the Hermes host, not a security sandbox.

Review the routing requirement, unsaved-editor expiry and disable/uninstall recovery instructions above before approving installation. Catalog submission is not catalog acceptance; only a maintainer-reviewed merge establishes that trust signal.

## Validation and development

Use the canonical Hermes `scripts/run_tests.sh` with this repository's `tests/` path; do not test against the real profile. Tests use disposable homes/Chromium, synthetic clocks and owned targets. `tests/sdk_probe.py` exercises real SDK discovery and unload in an explicitly marked isolated home. Keep private planning notes, state, profiles, logs/backups and host-specific routing out of distribution. Tests require pytest, pytest-asyncio and an installed Chromium; fixtures do not use your authenticated browser.

## License

MIT. See [LICENSE](LICENSE).
