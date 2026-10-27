"""One profile owner, hosted by a Hermes process. No process spawning."""
import asyncio
import atexit
import contextvars
import logging
import threading

from .service import Service
from .control import request, fence_cleanup
from .policy import settings_signature


class Runtime:
    def __init__(self, home, settings, policy, cleanup_error):
        self.home = home
        self.state = home / 'state' / 'chrome-tab-watchdog'
        self.settings = settings
        self.policy = policy
        self.cleanup_error = cleanup_error
        self.signature = settings_signature(settings, policy, cleanup_error)
        self.service = None
        self.loop = None
        self.task = None
        self.status = 'starting'
        self.stopped = threading.Event()
        self.ready = threading.Event()
        context = contextvars.copy_context()
        self.thread = threading.Thread(target=context.run, args=(self._run,),
                                       name='chrome-tab-watchdog', daemon=True)

    def start(self):
        atexit.register(self.stop)
        try:
            self.thread.start()
        except BaseException:
            atexit.unregister(self.stop)
            raise

    def stop(self):
        self.stopped.set()
        loop, task = self.loop, self.task
        if loop is not None and task is not None:
            try:
                loop.call_soon_threadsafe(task.cancel)
            except RuntimeError:
                pass
        if self.thread.ident is not None and threading.current_thread() is not self.thread:
            # Network operations have bounded timeouts. Joining guarantees no old
            # owner or loop survives an unload/reload to mutate the new owner's state.
            self.thread.join()
        atexit.unregister(self.stop)

    def _run(self):
        try:
            asyncio.run(self._serve())
        except asyncio.CancelledError:
            pass
        except Exception:
            self.status = 'startup_failed'
            logging.getLogger(__name__).error('Chrome tab watchdog runtime unavailable; cleanup disabled.')
        finally:
            self.loop = self.task = None
            self.ready.set()

    async def _serve(self):
        self.loop = asyncio.get_running_loop()
        self.task = asyncio.current_task()
        fenced_owner = None
        while not self.stopped.is_set():
            try:
                service = Service(self.state, self.settings['upstream'], self.settings['proxy_port'],
                                  self.policy, self.settings['proxy_host'], self.cleanup_error)
            except BlockingIOError:
                # Only the elected owner may open Store, unlink sockets or reset
                # coverage. Retry ownership if the hosting process goes away.
                self.status = 'attached'
                try:
                    health = await asyncio.to_thread(request, self.state, 'health', timeout=2)
                    owner = (health.get('runtime_pid'), health.get('runtime_incarnation'), health.get('configuration_signature'))
                    if (self.cleanup_error or owner[2] != self.signature) and owner != fenced_owner:
                        cause = 'owner_invalid_settings' if self.cleanup_error else 'owner_settings_conflict'
                        await asyncio.to_thread(fence_cleanup, self.state, reason=cause)
                        await asyncio.to_thread(request, self.state, 'fence', timeout=2, cause=cause)
                        fenced_owner = owner
                except Exception:
                    # A read-only owner probe is not browser activity. Retry it;
                    # poisoning the owner here makes a transient timeout permanent.
                    # Known-invalid local settings still require a fail-closed fence.
                    if self.cleanup_error:
                        await asyncio.to_thread(fence_cleanup, self.state, reason='owner_invalid_settings')
                self.ready.set()
                await asyncio.sleep(1)
                continue
            self.service = service
            service.configuration_signature = self.signature
            try:
                await service.start()
                self.status = 'owner'
                self.ready.set()
                while not self.stopped.is_set():
                    await asyncio.sleep(3600)
            finally:
                await service.stop()
                self.service = None
