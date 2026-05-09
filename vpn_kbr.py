"""Primary entrypoint for VPN_KBR bot."""

from kbrbot.app import client, configure_logging, log_runtime_version, loop, main, startup_cleanup


if __name__ == "__main__":
    startup_cleanup()
    configure_logging()
    log_runtime_version()
    with client:
        loop.run_until_complete(main())
